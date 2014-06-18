/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include <node.h>

#include "nlmdb.h"
#include "database.h"
#include "batch_async.h"
#include "batch.h"

namespace nlmdb {

BatchOp::BatchOp (v8::Local<v8::Object> &keyHandle, MDB_val key) : key(key) {
  NanScope();

  v8::Local<v8::Object> obj = NanNew<v8::Object>();
  obj->Set(NanNew("key"), keyHandle);
  NanAssignPersistent(persistentHandle, obj);
}

BatchOp::~BatchOp () {
  NanScope();

  v8::Local<v8::Object> handle = NanNew(persistentHandle);
  v8::Local<v8::Object> keyHandle =
      handle->Get(NanNew("key")).As<v8::Object>();
  DisposeStringOrBufferFromMDVal(keyHandle, key);

  if (!persistentHandle.IsEmpty())
    NanDisposePersistent(persistentHandle);
}

BatchDel::BatchDel (v8::Local<v8::Object> &keyHandle, MDB_val key)
  : BatchOp(keyHandle, key) {}

BatchDel::~BatchDel () {}

int BatchDel::Execute (MDB_txn *txn, MDB_dbi dbi) {
  return mdb_del(txn, dbi, &key, 0);
}

BatchPut::BatchPut (
    v8::Local<v8::Object> &keyHandle
  , MDB_val key
  , v8::Local<v8::Object> &valueHandle
  , MDB_val value
) : BatchOp(keyHandle, key)
  , value(value)
{
    v8::Local<v8::Object> handle = NanNew(persistentHandle);
    handle->Set(NanNew("value"), valueHandle);
}

BatchPut::~BatchPut () {
  NanScope();

  v8::Local<v8::Object> handle = NanNew(persistentHandle);
  v8::Local<v8::Object> valueHandle =
      handle->Get(NanNew("value")).As<v8::Object>();

  DisposeStringOrBufferFromMDVal(valueHandle, value);
}

int BatchPut::Execute (MDB_txn *txn, MDB_dbi dbi) {
  return mdb_put(txn, dbi, &key, &value, 0);
}

WriteBatch::WriteBatch (Database* database) : database(database) {
  operations = new std::vector<BatchOp*>;
  written = false;
}

WriteBatch::~WriteBatch () {
  Clear();
  delete operations;
}

void WriteBatch::Write (v8::Local<v8::Function> callback) {
  NanScope();

  written = true;

  if (operations->size() > 0) {
    NanAsyncQueueWorker(new BatchWriteWorker(
        this
      , new NanCallback(callback)
    ));
  } else {
    NL_RUN_CALLBACK(callback, NULL, 0);
  }
}

void WriteBatch::Put (
      v8::Local<v8::Object> &keyHandle
    , MDB_val key
    , v8::Local<v8::Object> &valueHandle
    , MDB_val value) {

  operations->push_back(new BatchPut(keyHandle, key, valueHandle, value));
}

void WriteBatch::Delete (v8::Local<v8::Object> &keyHandle, MDB_val key) {
  operations->push_back(new BatchDel(keyHandle, key));
}

void WriteBatch::Clear () {
    for (std::vector< BatchOp* >::iterator it = operations->begin()
      ; it != operations->end()
      ; ) {

    delete *it;
    it = operations->erase(it);
  }
}

static v8::Persistent<v8::FunctionTemplate> writebatch_constructor;

void WriteBatch::Init () {
  NanScope();

  v8::Local<v8::FunctionTemplate> tpl = NanNew<v8::FunctionTemplate>(WriteBatch::New);
  NanAssignPersistent(writebatch_constructor, tpl);
  tpl->SetClassName(NanNew("Batch"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  NODE_SET_PROTOTYPE_METHOD(tpl, "put", WriteBatch::Put);
  NODE_SET_PROTOTYPE_METHOD(tpl, "del", WriteBatch::Del);
  NODE_SET_PROTOTYPE_METHOD(tpl, "clear", WriteBatch::Clear);
  NODE_SET_PROTOTYPE_METHOD(tpl, "write", WriteBatch::Write);
}

NAN_METHOD(WriteBatch::New) {
  NanScope();

  Database* database = node::ObjectWrap::Unwrap<Database>(args[0]->ToObject());
  v8::Local<v8::Object> optionsObj;

  if (args.Length() > 1 && args[1]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(args[1]);
  }

  WriteBatch* batch = new WriteBatch(database);
  batch->Wrap(args.This());

  NanReturnValue(args.This());
}

v8::Handle<v8::Value> WriteBatch::NewInstance (
        v8::Handle<v8::Object> database
      , v8::Handle<v8::Object> optionsObj = v8::Handle<v8::Object>()
    ) {

  NanEscapableScope();

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      NanNew(writebatch_constructor);

  if (optionsObj.IsEmpty()) {
    v8::Handle<v8::Value> argv[] = { database };
    instance = constructorHandle->GetFunction()->NewInstance(1, argv);
  } else {
    v8::Handle<v8::Value> argv[] = { database, optionsObj };
    instance = constructorHandle->GetFunction()->NewInstance(2, argv);
  }

  return NanEscapeScope(instance);
}

NAN_METHOD(WriteBatch::Put) {
  NanScope();

  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(args.Holder());

  if (batch->written) {
    return NanThrowError("write() already called on this batch");
  }

  v8::Handle<v8::Function> callback; // purely for the error macros

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[0], key)
  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[1], value)

  v8::Local<v8::Object> keyHandle = args[0].As<v8::Object>();
  v8::Local<v8::Object> valueHandle = args[1].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)
  NL_STRING_OR_BUFFER_TO_MDVAL(value, valueHandle, value)

  batch->Put(keyHandle, key, valueHandle, value);

  NanReturnValue(args.Holder());
}

NAN_METHOD(WriteBatch::Del) {
  NanScope();

  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(args.Holder());

  if (batch->written) {
    return NanThrowError("write() already called on this batch");
  }

  v8::Handle<v8::Function> callback; // purely for the error macros

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[0], key)

  v8::Local<v8::Object> keyHandle = args[0].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)

  batch->Delete(keyHandle, key);

  NanReturnValue(args.Holder());
}

NAN_METHOD(WriteBatch::Clear) {
  NanScope();

  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(args.Holder());

  if (batch->written) {
    return NanThrowError("write() already called on this batch");
  }

  batch->Clear();

  NanReturnValue(args.Holder());
}

NAN_METHOD(WriteBatch::Write) {
  NanScope();

  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(args.Holder());

  if (args.Length() == 0 || !args[0]->IsFunction()) {
    return NanThrowError("write() requires a callback argument");
  }

  if (batch->written) {
    return NanThrowError("write() already called on this batch");
  }
  
  if (args.Length() == 0) {
    return NanThrowError("write() requires a callback argument");
  }

  batch->Write(args[0].As<v8::Function>());

  NanReturnUndefined();
}

} // namespace nlmdb

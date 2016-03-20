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
  Nan::HandleScope scope;

  v8::Local<v8::Object> obj = Nan::New<v8::Object>();
  obj->Set(Nan::New("key").ToLocalChecked(), keyHandle);
  persistentHandle.Reset(obj);
}

BatchOp::~BatchOp () {
  Nan::HandleScope scope;

  v8::Local<v8::Object> handle = Nan::New<v8::Object>(persistentHandle);
  v8::Local<v8::Object> keyHandle =
      handle->Get(Nan::New("key").ToLocalChecked()).As<v8::Object>();
  DisposeStringOrBufferFromMDVal(keyHandle, key);

  if (!persistentHandle.IsEmpty())
    persistentHandle.Reset();
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
  Nan::HandleScope scope;
  v8::Local<v8::Object> handle = Nan::New<v8::Object>(persistentHandle);
  handle->Set(Nan::New("value").ToLocalChecked(), valueHandle);
}

BatchPut::~BatchPut () {
  Nan::HandleScope scope;

  v8::Local<v8::Object> handle = Nan::New<v8::Object>(persistentHandle);
  v8::Local<v8::Object> valueHandle =
      handle->Get(Nan::New("value").ToLocalChecked()).As<v8::Object>();

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
  Nan::HandleScope scope;

  written = true;

  if (operations->size() > 0) {
    Nan::AsyncQueueWorker(new BatchWriteWorker(
        this
      , new Nan::Callback(callback)
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

static Nan::Persistent<v8::FunctionTemplate> writebatch_constructor;

void WriteBatch::Init () {
  Nan::HandleScope scope;

  v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(WriteBatch::New);
  writebatch_constructor.Reset(tpl);
  tpl->SetClassName(Nan::New("Batch").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  Nan::SetPrototypeMethod(tpl, "put", WriteBatch::Put);
  Nan::SetPrototypeMethod(tpl, "del", WriteBatch::Del);
  Nan::SetPrototypeMethod(tpl, "clear", WriteBatch::Clear);
  Nan::SetPrototypeMethod(tpl, "write", WriteBatch::Write);
}

NAN_METHOD(WriteBatch::New) {
  Database* database = Nan::ObjectWrap::Unwrap<Database>(info[0]->ToObject());
  v8::Local<v8::Object> optionsObj;

  if (info.Length() > 1 && info[1]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(info[1]);
  }

  WriteBatch* batch = new WriteBatch(database);
  batch->Wrap(info.This());

  info.GetReturnValue().Set(info.This());
}

v8::Handle<v8::Value> WriteBatch::NewInstance (
        v8::Handle<v8::Object> database
      , v8::Handle<v8::Object> optionsObj = v8::Handle<v8::Object>()
    ) {
  Nan::EscapableHandleScope scope;

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      Nan::New<v8::FunctionTemplate>(writebatch_constructor);

  if (optionsObj.IsEmpty()) {
    v8::Handle<v8::Value> argv[] = { database };
    instance = constructorHandle->GetFunction()->NewInstance(1, argv);
  } else {
    v8::Handle<v8::Value> argv[] = { database, optionsObj };
    instance = constructorHandle->GetFunction()->NewInstance(2, argv);
  }

  return scope.Escape(instance);
}

NAN_METHOD(WriteBatch::Put) {
  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(info.Holder());

  if (batch->written) {
    return Nan::ThrowError("write() already called on this batch");
  }

  v8::Handle<v8::Function> callback; // purely for the error macros

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[0], key)
  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[1], value)

  v8::Local<v8::Object> keyHandle = info[0].As<v8::Object>();
  v8::Local<v8::Object> valueHandle = info[1].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)
  NL_STRING_OR_BUFFER_TO_MDVAL(value, valueHandle, value)

  batch->Put(keyHandle, key, valueHandle, value);

  info.GetReturnValue().Set(info.Holder());
}

NAN_METHOD(WriteBatch::Del) {
  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(info.Holder());

  if (batch->written) {
    return Nan::ThrowError("write() already called on this batch");
  }

  v8::Handle<v8::Function> callback; // purely for the error macros

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[0], key)

  v8::Local<v8::Object> keyHandle = info[0].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)

  batch->Delete(keyHandle, key);

  info.GetReturnValue().Set(info.Holder());
}

NAN_METHOD(WriteBatch::Clear) {
  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(info.Holder());

  if (batch->written) {
    return Nan::ThrowError("write() already called on this batch");
  }

  batch->Clear();

  info.GetReturnValue().Set(info.Holder());
}

NAN_METHOD(WriteBatch::Write) {
  WriteBatch* batch = ObjectWrap::Unwrap<WriteBatch>(info.Holder());

  if (info.Length() == 0 || !info[0]->IsFunction()) {
    return Nan::ThrowError("write() requires a callback argument");
  }

  if (batch->written) {
    return Nan::ThrowError("write() already called on this batch");
  }

  if (info.Length() == 0) {
    return Nan::ThrowError("write() requires a callback argument");
  }

  batch->Write(info[0].As<v8::Function>());

  info.GetReturnValue().SetUndefined();
}

} // namespace nlmdb

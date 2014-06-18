/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include <node.h>
#include <node_buffer.h>

#include "database.h"
#include "nlmdb.h"
#include "database_async.h"

namespace nlmdb {

/** OPEN WORKER **/

OpenWorker::OpenWorker (
    Database* database
  , NanCallback *callback
  , OpenOptions options
) : AsyncWorker(database, callback)
  , options(options)
{ };

OpenWorker::~OpenWorker () {}

void OpenWorker::Execute () {
  SetStatus(database->OpenDatabase(options));
}

/** CLOSE WORKER **/

CloseWorker::CloseWorker (
    Database* database
  , NanCallback *callback
) : AsyncWorker(database, callback)
{};

CloseWorker::~CloseWorker () {}

void CloseWorker::Execute () {
  database->CloseDatabase();
}

void CloseWorker::WorkComplete () {
/*
  NanScope();
  HandleOKCallback();
*/
  AsyncWorker::WorkComplete();
}

/** IO WORKER (abstract) **/
IOWorker::IOWorker (
    Database* database
  , NanCallback *callback
  , MDB_val key
  , v8::Local<v8::Object> &keyHandle
) : AsyncWorker(database, callback)
  , key(key)
{
  NanScope();

  SaveToPersistent("key", keyHandle);
};

IOWorker::~IOWorker () {}

void IOWorker::WorkComplete () {
  NanScope();

  DisposeStringOrBufferFromMDVal(GetFromPersistent("key"), key);
  AsyncWorker::WorkComplete();
}

/** READ WORKER **/
ReadWorker::ReadWorker (
    Database* database
  , NanCallback *callback
  , MDB_val key
  , bool asBuffer
  , v8::Local<v8::Object> &keyHandle
) : IOWorker(database, callback, key, keyHandle)
  , asBuffer(asBuffer)
{};

ReadWorker::~ReadWorker () {}

void ReadWorker::Execute () {
  SetStatus(database->GetFromDatabase(key, value));
}

void ReadWorker::HandleOKCallback () {
  NanScope();
  v8::Local<v8::Value> returnValue;
  if (asBuffer) {
    returnValue = NanNewBufferHandle((char*)value.mv_data, value.mv_size);
  } else {
    returnValue = NanNew((char*)value.mv_data, value.mv_size);
  }
  v8::Local<v8::Value> argv[] = {
      NanNull()
    , returnValue
  };
  callback->Call(2, argv);
}

/** DELETE WORKER **/
DeleteWorker::DeleteWorker (
    Database* database
  , NanCallback *callback
  , MDB_val key
  , v8::Local<v8::Object> &keyHandle
) : IOWorker(database, callback, key, keyHandle)
{ };

DeleteWorker::~DeleteWorker () {}

void DeleteWorker::Execute () {
  SetStatus(database->DeleteFromDatabase(key));
}

void DeleteWorker::WorkComplete () {
  NanScope();

  if (status.code == MDB_NOTFOUND || (status.code == 0 && status.error.length() == 0))
    HandleOKCallback();
  else
    HandleErrorCallback();

  // IOWorker does this but we can't call IOWorker::WorkComplete()
  DisposeStringOrBufferFromMDVal(GetFromPersistent("key"), key);
  delete callback;
  callback = NULL;
}

/** WRITE WORKER **/
WriteWorker::WriteWorker (
    Database* database
  , NanCallback *callback
  , MDB_val key
  , MDB_val value
  , v8::Local<v8::Object> &keyHandle
  , v8::Local<v8::Object> &valueHandle
) : DeleteWorker(database, callback, key, keyHandle)
  , value(value)
  , valueHandle(valueHandle)
{
  NanScope();

  SaveToPersistent("value", valueHandle);
};

WriteWorker::~WriteWorker () {}

void WriteWorker::Execute () {
  SetStatus(database->PutToDatabase(key, value));
}

void WriteWorker::WorkComplete () {
  NanScope();

  DisposeStringOrBufferFromMDVal(GetFromPersistent("value"), value);
  IOWorker::WorkComplete();
}

} // namespace nlmdb

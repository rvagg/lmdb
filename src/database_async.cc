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
  , Nan::Callback *callback
  , OpenOptions options
) : AsyncWorker(database, callback)
  , options(options)
{ };

OpenWorker::~OpenWorker () {
  delete callback;
  callback = NULL;
}

void OpenWorker::Execute () {
  SetStatus(database->OpenDatabase(options));
}

/** CLOSE WORKER **/

CloseWorker::CloseWorker (
    Database* database
  , Nan::Callback *callback
) : AsyncWorker(database, callback)
{};

CloseWorker::~CloseWorker () {
  delete callback;
  callback = NULL;
}

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
  , Nan::Callback *callback
  , MDB_val key
  , v8::Local<v8::Object> &keyHandle
) : AsyncWorker(database, callback)
  , key(key)
  , keyHandle(keyHandle)
{
  SaveToPersistent("key", keyHandle);
};

IOWorker::~IOWorker () {}

void IOWorker::WorkComplete () {
  Nan::HandleScope scope;

  DisposeStringOrBufferFromMDVal(GetFromPersistent("key"), key);
  AsyncWorker::WorkComplete();
}

/** READ WORKER **/
ReadWorker::ReadWorker (
    Database* database
  , Nan::Callback *callback
  , MDB_val key
  , bool asBuffer
  , v8::Local<v8::Object> &keyHandle
) : IOWorker(database, callback, key, keyHandle)
  , asBuffer(asBuffer)
{};

ReadWorker::~ReadWorker () {
  delete callback;
  callback = NULL;
}

void ReadWorker::Execute () {
  SetStatus(database->GetFromDatabase(key, value));
}

void ReadWorker::HandleOKCallback () {
  Nan::HandleScope scope;

  v8::Local<v8::Value> returnValue;
  if (asBuffer) {
    returnValue = Nan::CopyBuffer((char*)value.mv_data, value.mv_size).ToLocalChecked();
  } else {
    returnValue = Nan::New<v8::String>((char*)value.mv_data, value.mv_size).ToLocalChecked();
  }
  v8::Local<v8::Value> argv[] = {
      Nan::Null()
    , returnValue
  };
  callback->Call(2, argv);
}

/** DELETE WORKER **/
DeleteWorker::DeleteWorker (
    Database* database
  , Nan::Callback *callback
  , MDB_val key
  , v8::Local<v8::Object> &keyHandle
) : IOWorker(database, callback, key, keyHandle)
{};

DeleteWorker::~DeleteWorker () {
  delete callback;
  callback = NULL;
}

void DeleteWorker::Execute () {
  SetStatus(database->DeleteFromDatabase(key));
}

void DeleteWorker::WorkComplete () {
  Nan::HandleScope scope;

  if (status.code == MDB_NOTFOUND || (status.code == 0 && status.error.length() == 0))
    HandleOKCallback();
  else
    HandleErrorCallback();

  // IOWorker does this but we can't call IOWorker::WorkComplete()
  DisposeStringOrBufferFromMDVal(GetFromPersistent("key"), key);
}

/** WRITE WORKER **/
WriteWorker::WriteWorker (
    Database* database
  , Nan::Callback *callback
  , MDB_val key
  , MDB_val value
  , v8::Local<v8::Object> &keyHandle
  , v8::Local<v8::Object> &valueHandle
) : DeleteWorker(database, callback, key, keyHandle)
  , value(value)
  , valueHandle(valueHandle)
{
  SaveToPersistent("value", valueHandle);
};

WriteWorker::~WriteWorker () {
  delete callback;
  callback = NULL;
}

void WriteWorker::Execute () {
  SetStatus(database->PutToDatabase(key, value));
}

void WriteWorker::WorkComplete () {
  Nan::HandleScope scope;

  DisposeStringOrBufferFromMDVal(GetFromPersistent("value"), value);
  IOWorker::WorkComplete();
}

} // namespace nlmdb

/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef NL_DATABASE_ASYNC_H
#define NL_DATABASE_ASYNC_H

#include <vector>
#include <node.h>
#include <nan.h>

#include "async.h"

namespace nlmdb {

class OpenWorker : public AsyncWorker {
public:
  OpenWorker (
      Database* database
    , NanCallback *callback
    , OpenOptions options
  );

  virtual ~OpenWorker ();
  virtual void Execute ();

private:
  OpenOptions options;
};

class CloseWorker : public AsyncWorker {
public:
  CloseWorker (
      Database* database
    , NanCallback *callback
  );

  virtual ~CloseWorker ();
  virtual void Execute ();
  virtual void WorkComplete ();
};

class IOWorker    : public AsyncWorker {
public:
  IOWorker (
      Database* database
    , NanCallback *callback
    , MDB_val key
    , v8::Local<v8::Object> &keyHandle
  );

  virtual ~IOWorker ();
  virtual void WorkComplete ();

protected:
  MDB_val key;
};

class ReadWorker : public IOWorker {
public:
  ReadWorker (
      Database* database
    , NanCallback *callback
    , MDB_val key
    , bool asBuffer
    , v8::Local<v8::Object> &keyHandle
  );

  virtual ~ReadWorker ();
  virtual void Execute ();
  virtual void HandleOKCallback ();

private:
  bool asBuffer;
  MDB_val value;
};

class DeleteWorker : public IOWorker {
public:
  DeleteWorker (
      Database* database
    , NanCallback *callback
    , MDB_val key
    , v8::Local<v8::Object> &keyHandle
  );

  virtual ~DeleteWorker ();
  virtual void Execute ();
  virtual void WorkComplete ();

protected:
};

class WriteWorker : public DeleteWorker {
public:
  WriteWorker (
      Database* database
    , NanCallback *callback
    , MDB_val key
    , MDB_val value
    , v8::Local<v8::Object> &keyHandle
    , v8::Local<v8::Object> &valueHandle
  );

  virtual ~WriteWorker ();
  virtual void Execute ();
  virtual void WorkComplete ();

private:
  MDB_val value;
  v8::Local<v8::Object> &valueHandle;
};

/*
class BatchWorker : public AsyncWorker {
public:
  BatchWorker (
      Database* database
    , NanCallback *callback
    , leveldb::WriteBatch* batch
    , std::vector<Reference>* references
    , bool sync
  );

  virtual ~BatchWorker ();
  virtual void Execute ();

private:
  leveldb::WriteOptions* options;
  leveldb::WriteBatch* batch;
  std::vector<Reference>* references;
};

class ApproximateSizeWorker : public AsyncWorker {
public:
  ApproximateSizeWorker (
      Database* database
    , NanCallback *callback
    , MDB_val start
    , MDB_val end
    , v8::Local<v8::Object> startHandle
    , v8::Local<v8::Object> endHandle
  );

  virtual ~ApproximateSizeWorker ();
  virtual void Execute ();
  virtual void HandleOKCallback ();
  virtual void WorkComplete ();

  private:
    leveldb::Range range;
    v8::Local<v8::Object> startHandle;
    v8::Local<v8::Object> endHandle;
    uint64_t size;
};
*/
} // namespace nlmdb

#endif

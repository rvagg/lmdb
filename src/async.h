/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef LD_ASYNC_H
#define LD_ASYNC_H

#include <node.h>
#include "nan.h"
#include "database.h"

namespace nlmdb {

class Database;

/* abstract */ class AsyncWorker : public NanAsyncWorker {
public:
  AsyncWorker (
      nlmdb::Database* database
    , NanCallback *callback
  ) : NanAsyncWorker(callback), database(database) {
    NanScope();
    v8::Local<v8::Object> obj = NanNew<v8::Object>();
    NanAssignPersistent(persistentHandle, obj);
  }

protected:
  void SetStatus (md_status status) {
    this->status = status;

    if (status.error.length() != 0) {
      char *e = new char[status.error.length() + 1];
      strcpy(e, status.error.c_str());
      this->SetErrorMessage(e);
    } else if (status.code != 0) {
      const char *me = mdb_strerror(status.code);
      char *e = new char[strlen(me) + 1];
      strcpy(e, me);
      this->SetErrorMessage(e);
    }
  }

  void SetStatus (int code) {
    md_status status;
    status.code = code;
    SetStatus(status);
  }

  Database* database;

  md_status status;
};

} // namespace nlmdb

#endif

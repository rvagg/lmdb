/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef NL_ITERATOR_ASYNC_H
#define NL_ITERATOR_ASYNC_H

#include <node.h>
#include <nan.h>

#include "async.h"
#include "iterator.h"

namespace nlmdb {

class NextWorker : public AsyncWorker {
public:
  NextWorker (
      Iterator* iterator
    , Nan::Callback *callback
    , void (*localCallback)(Iterator*)
  );

  virtual ~NextWorker ();
  virtual void Execute ();
  virtual void HandleOKCallback ();
  virtual void WorkComplete ();

private:
  Iterator *iterator;
  void (*localCallback)(Iterator*);
  MDB_val key;
  MDB_val value;
};

class EndWorker : public AsyncWorker {
public:
  EndWorker (
      Iterator* iterator
    , Nan::Callback *callback
  );

  virtual ~EndWorker ();
  virtual void Execute ();
  virtual void HandleOKCallback ();

bool executed;
private:
  Iterator* iterator;
};

} // namespace nlmdb

#endif

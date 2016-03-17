/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef NL_BATCH_ASYNC_H
#define NL_BATCH_ASYNC_H

#include <node.h>
#include <nan.h>

#include "async.h"
#include "batch.h"
#include "database.h"

namespace nlmdb {

class BatchWriteWorker : public AsyncWorker {
public:
  BatchWriteWorker (WriteBatch* batch, Nan::Callback *callback);

  virtual ~BatchWriteWorker ();
  virtual void Execute ();

private:
  WriteBatch* batch;
};

} // namespace nlmdb

#endif

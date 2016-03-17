/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include "batch.h"
#include "batch_async.h"

namespace nlmdb {

BatchWriteWorker::BatchWriteWorker (
    WriteBatch* batch
  , Nan::Callback *callback
) : AsyncWorker(batch->database, callback)
  , batch(batch)
{};

BatchWriteWorker::~BatchWriteWorker () { }

void BatchWriteWorker::Execute () {
  SetStatus(database->PutToDatabase(batch->operations));
}

} // namespace nlmdb

/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include "nlmdb.h"
#include "database.h"
#include "batch.h"
#include "iterator.h"

namespace nlmdb {

NAN_MODULE_INIT(init) {
  Nan::HandleScope scope;

  Database::Init();
  WriteBatch::Init();
  Iterator::Init();

  v8::Local<v8::Function> nlmdb =
      Nan::GetFunction(Nan::New<v8::FunctionTemplate>(NLMDB)).ToLocalChecked();

  target->Set(Nan::New("nlmdb").ToLocalChecked(), nlmdb);
}

NODE_MODULE(nlmdb, init)

} // namespace nlmdb

/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include <node.h>
#include <node_buffer.h>

#include "database.h"
#include "nlmdb.h"
#include "async.h"
#include "iterator_async.h"

namespace nlmdb {

/** NEXT WORKER **/

NextWorker::NextWorker (
    Iterator* iterator
  , Nan::Callback *callback
  , void (*localCallback)(Iterator*)
) : AsyncWorker(NULL, callback)
  , iterator(iterator)
  , localCallback(localCallback)
{};

NextWorker::~NextWorker () {
  delete callback;
  callback = NULL;
}

void NextWorker::Execute () {
//std::cerr << "NextWorker::Execute: " << iterator->id << std::endl;
  SetStatus(iterator->Next(&key, &value));
//std::cerr << "NextWorker::Execute done: " << iterator->id << std::endl;
}

void NextWorker::WorkComplete () {
  Nan::HandleScope scope;

  if (status.code == MDB_NOTFOUND || (status.code == 0 && status.error.length() == 0))
    HandleOKCallback();
  else
    HandleErrorCallback();
}

void NextWorker::HandleOKCallback () {
  Nan::HandleScope scope;

//std::cerr << "NextWorker::HandleOKCallback: " << iterator->id << std::endl;
//std::cerr << "Read [" << std::string((char*)key.mv_data, key.mv_size) << "]=[" << std::string((char*)value.mv_data, value.mv_size) << "]\n";

  if (status.code == MDB_NOTFOUND) {
    //std::cerr << "run callback, ended MDB_NOTFOUND\n";
    localCallback(iterator);
    callback->Call(0, NULL);
    return;
  }

  v8::Local<v8::Value> returnKey;
  if (iterator->keyAsBuffer) {
    returnKey = Nan::CopyBuffer((char*)key.mv_data, key.mv_size).ToLocalChecked();
  } else {
    returnKey = Nan::New<v8::String>((char*)key.mv_data, key.mv_size).ToLocalChecked();
  }

  v8::Local<v8::Value> returnValue;
  if (iterator->valueAsBuffer) {
    returnValue = Nan::CopyBuffer((char*)value.mv_data, value.mv_size).ToLocalChecked();
  } else {
    returnValue = Nan::New<v8::String>((char*)value.mv_data, value.mv_size).ToLocalChecked();
  }

  // clean up & handle the next/end state see iterator.cc/checkEndCallback
  //std::cerr << "run callback, ended FOUND\n";
  localCallback(iterator);

  v8::Local<v8::Value> argv[] = {
      Nan::Null()
    , returnKey
    , returnValue
  };

  callback->Call(3, argv);
}

/** END WORKER **/

EndWorker::EndWorker (
    Iterator* iterator
  , Nan::Callback *callback
) : AsyncWorker(NULL, callback)
  , iterator(iterator)
{executed=false;};

EndWorker::~EndWorker () {
  delete callback;
  callback = NULL;
}

void EndWorker::Execute () {
  executed = true;
  //std::cerr << "EndWorker::Execute...\n";
  iterator->End();
}

void EndWorker::HandleOKCallback () {
  //std::cerr << "EndWorker::HandleOKCallback: " << iterator->id << std::endl;
  iterator->Release();
  callback->Call(0, NULL);
}

} // namespace nlmdb

/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include <node.h>
#include <node_buffer.h>
#include <string.h>
#include <nan.h>

#include "database.h"
#include "iterator.h"
#include "iterator_async.h"

namespace nlmdb {

inline int compare(std::string *str, MDB_val *value) {
  const int min_len = (str->length() < value->mv_size)
      ? str->length()
      : value->mv_size;
  int r = memcmp(str->c_str(), (char *)value->mv_data, min_len);
  if (r == 0) {
    if (str->length() < value->mv_size) r = -1;
    else if (str->length() > value->mv_size) r = +1;
  }
  return r;
}

Iterator::Iterator (
    Database    *database
  , uint32_t     id
  , std::string *start
  , std::string *end
  , bool         reverse
  , bool         keys
  , bool         values
  , int          limit
  , bool         keyAsBuffer
  , bool         valueAsBuffer
) : database(database)
  , id(id)
  , start(start)
  , end(end)
  , reverse(reverse)
  , keys(keys)
  , values(values)
  , limit(limit)
  , keyAsBuffer(keyAsBuffer)
  , valueAsBuffer(valueAsBuffer)
{
  count     = 0;
  started   = false;
  nexting   = false;
  ended     = false;
  endWorker = NULL;
};

Iterator::~Iterator () {
  if (start != NULL)
    delete start;
  if (end != NULL)
    delete end;
};

int Iterator::Next (MDB_val *key, MDB_val *value) {
  //std::cerr << "Iterator::Next " << started << ", " << id << std::endl;
  int rc = 0;

  if (!started) {
    //std::cerr << "opening cursor... " << std::endl;
    rc = database->NewIterator(&txn, &cursor);
    //std::cerr << "opened cursor!! " << cursor << ", " << strerror(rc) << std::endl;
    if (rc) {
      //std::cerr << "returning 0: " << rc << std::endl;
      return rc;
    }

    if (start != NULL) {
      key->mv_data = (void*)start->data();
      key->mv_size = start->length();
      rc = mdb_cursor_get(cursor, key, value, MDB_SET_RANGE);
      if (reverse) {
        if (rc == MDB_NOTFOUND)
          rc = mdb_cursor_get(cursor, key, value, MDB_LAST);
        else if (rc == 0 && compare(start, key))
          rc = mdb_cursor_get(cursor, key, value, MDB_PREV);
      }
    } else if (reverse) {
      rc = mdb_cursor_get(cursor, key, value, MDB_LAST);
    } else {
      rc = mdb_cursor_get(cursor, key, value, MDB_FIRST);
    }

    started = true;
    //std::cerr << "Started " << started << std::endl;
  } else {
    //std::cerr << "started! getting cursor..." << std::endl;
    if (reverse)
      rc = mdb_cursor_get(cursor, key, value, MDB_PREV);
    else
      rc = mdb_cursor_get(cursor, key, value, MDB_NEXT);
    //std::cerr << "started! got cursor..." << std::endl;
  }

  if (rc) {
    //std::cerr << "returning 1: " << rc << std::endl;
    return rc;
  }

  //std::cerr << "***" << std::string((const char*)key->mv_data, key->mv_size) << std::endl;
  //if (end != NULL)
    //std::cerr << "***end=" << end->c_str() << ", " << reverse << ", " << compare(end, key) << std::endl;

  // 'end' here is an inclusive test
  if ((limit < 0 || ++count <= limit)
      && (end == NULL
          || (reverse && compare(end, key) <= 0)
          || (!reverse && compare(end, key) >= 0))) {
    return 0; // good to continue
  }

  key = 0;
  value = 0;
  return MDB_NOTFOUND;
}

void Iterator::End () {
  //std::cerr << "Iterator::End " << started << ", " << id << std::endl;
  if (started) {
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
  }
}

void Iterator::Release () {
  //std::cerr << "Iterator::Release " << started << ", " << id << std::endl;
  database->ReleaseIterator(id);
}

void checkEndCallback (Iterator* iterator) {
  iterator->nexting = false;
  if (iterator->endWorker != NULL) {
    Nan::AsyncQueueWorker(iterator->endWorker);
    iterator->endWorker = NULL;
  }
}

NAN_METHOD(Iterator::Next) {
  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());

  if (info.Length() == 0 || !info[0]->IsFunction()) {
    return Nan::ThrowError("next() requires a callback argument");
  }

  v8::Local<v8::Function> callback = info[0].As<v8::Function>();

  if (iterator->ended) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "cannot call next() after end()")
  }

  if (iterator->nexting) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "cannot call next() before previous next() has completed")
  }

  NextWorker* worker = new NextWorker(
      iterator
    , new Nan::Callback(callback)
    , checkEndCallback
  );
  iterator->nexting = true;
  Nan::AsyncQueueWorker(worker);

  info.GetReturnValue().Set(info.Holder());
}

NAN_METHOD(Iterator::End) {
  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());
  //std::cerr << "Iterator::End" << iterator->id << ", " << iterator->nexting << ", " << iterator->ended << std::endl;

  if (info.Length() == 0 || !info[0]->IsFunction()) {
    return Nan::ThrowError("end() requires a callback argument");
  }

  v8::Local<v8::Function> callback = v8::Local<v8::Function>::Cast(info[0]);

  if (iterator->ended) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "end() already called on iterator")
  }

  EndWorker* worker = new EndWorker(
      iterator
    , new Nan::Callback(callback)
  );
  iterator->ended = true;

  if (iterator->nexting) {
    // waiting for a next() to return, queue the end
    //std::cerr << "Iterator is nexting: " << iterator->id << std::endl;
    iterator->endWorker = worker;
  } else {
    //std::cerr << "Iterator can be ended: " << iterator->id << std::endl;
    Nan::AsyncQueueWorker(worker);
  }

  info.GetReturnValue().Set(info.Holder());
}

static Nan::Persistent<v8::FunctionTemplate> iterator_constructor;

void Iterator::Init () {
  Nan::HandleScope scope;

  v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(Iterator::New);
  iterator_constructor.Reset(tpl);
  tpl->SetClassName(Nan::New("Iterator").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  Nan::SetPrototypeMethod(tpl, "next", Iterator::Next);
  Nan::SetPrototypeMethod(tpl, "end", Iterator::End);
}

v8::Handle<v8::Object> Iterator::NewInstance (
        v8::Handle<v8::Object> database
      , v8::Handle<v8::Number> id
      , v8::Handle<v8::Object> optionsObj
    ) {

  Nan::HandleScope scope;

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      Nan::New(iterator_constructor);

  if (optionsObj.IsEmpty()) {
    v8::Handle<v8::Value> argv[] = { database, id };
    instance = constructorHandle->GetFunction()->NewInstance(2, argv);
  } else {
    v8::Handle<v8::Value> argv[] = { database, id, optionsObj };
    instance = constructorHandle->GetFunction()->NewInstance(3, argv);
  }

  return instance;
}

NAN_METHOD(Iterator::New) {
  Database* database = Nan::ObjectWrap::Unwrap<Database>(info[0]->ToObject());

  //TODO: remove this, it's only here to make NL_STRING_OR_BUFFER_TO_MDVAL happy
  v8::Handle<v8::Function> callback;

  std::string* start = NULL;
  std::string* end = NULL;
  int limit = -1;

  v8::Local<v8::Value> id = info[1];

  v8::Local<v8::Object> optionsObj;

  if (info.Length() > 1 && info[2]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(info[2]);

    if (optionsObj->Has(Nan::New("start").ToLocalChecked())
        && (node::Buffer::HasInstance(optionsObj->Get(Nan::New("start").ToLocalChecked()))
          || optionsObj->Get(Nan::New("start").ToLocalChecked())->IsString())) {

      v8::Local<v8::Value> startBuffer =
          Nan::New<v8::Value>(optionsObj->Get(Nan::New("start").ToLocalChecked()));

      // ignore start if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(startBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_start, startBuffer, start)
        start = new std::string((const char*)_start.mv_data, _start.mv_size);
      }
    }

    if (optionsObj->Has(Nan::New("end").ToLocalChecked())
        && (node::Buffer::HasInstance(optionsObj->Get(Nan::New("end").ToLocalChecked()))
          || optionsObj->Get(Nan::New("end").ToLocalChecked())->IsString())) {

      v8::Local<v8::Value> endBuffer =
          Nan::New<v8::Value>(optionsObj->Get(Nan::New("end").ToLocalChecked()));

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(endBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_end, endBuffer, end)
        end = new std::string((const char*)_end.mv_data, _end.mv_size);
      }
    }

    if (!optionsObj.IsEmpty() && optionsObj->Has(Nan::New("limit").ToLocalChecked())) {
      limit =
        v8::Local<v8::Integer>::Cast(optionsObj->Get(Nan::New("limit").ToLocalChecked()))->Value();
    }
  }

  bool reverse = BooleanOptionValueDef(optionsObj, Nan::New("reverse").ToLocalChecked(), false);
  bool keys = BooleanOptionValueDef(optionsObj, Nan::New("keys").ToLocalChecked(), true);
  bool values = BooleanOptionValueDef(optionsObj, Nan::New("values").ToLocalChecked(), true);
  bool keyAsBuffer = BooleanOptionValueDef(
      optionsObj
    , Nan::New("keyAsBuffer").ToLocalChecked()
    , true
  );
  bool valueAsBuffer = BooleanOptionValueDef(
      optionsObj
    , Nan::New("valueAsBuffer").ToLocalChecked()
    , false
  );

  Iterator* iterator = new Iterator(
      database
    , (uint32_t)id->Int32Value()
    , start
    , end
    , reverse
    , keys
    , values
    , limit
    , keyAsBuffer
    , valueAsBuffer
  );
  iterator->Wrap(info.This());

  //std::cerr << "New Iterator " << iterator->id << std::endl;

  info.GetReturnValue().Set(info.This());
}

} // namespace nlmdb

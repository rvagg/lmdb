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
    NanAsyncQueueWorker(iterator->endWorker);
    iterator->endWorker = NULL;
  }
}

NAN_METHOD(Iterator::Next) {
  NanScope();

  Iterator* iterator = node::ObjectWrap::Unwrap<Iterator>(args.This());

  if (args.Length() == 0 || !args[0]->IsFunction()) {
    return NanThrowError("next() requires a callback argument");
  }

  v8::Local<v8::Function> callback = args[0].As<v8::Function>();

  if (iterator->ended) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "cannot call next() after end()")
  }

  if (iterator->nexting) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "cannot call next() before previous next() has completed")
  }

  NextWorker* worker = new NextWorker(
      iterator
    , new NanCallback(callback)
    , checkEndCallback
  );
  iterator->nexting = true;
  NanAsyncQueueWorker(worker);

  NanReturnValue(args.Holder());
}

NAN_METHOD(Iterator::End) {
  NanScope();

  Iterator* iterator = node::ObjectWrap::Unwrap<Iterator>(args.This());
  //std::cerr << "Iterator::End" << iterator->id << ", " << iterator->nexting << ", " << iterator->ended << std::endl;

  if (args.Length() == 0 || !args[0]->IsFunction()) {
    return NanThrowError("end() requires a callback argument");
  }

  v8::Local<v8::Function> callback = v8::Local<v8::Function>::Cast(args[0]);

  if (iterator->ended) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "end() already called on iterator")
  }

  EndWorker* worker = new EndWorker(
      iterator
    , new NanCallback(callback)
  );
  iterator->ended = true;

  if (iterator->nexting) {
    // waiting for a next() to return, queue the end
    //std::cerr << "Iterator is nexting: " << iterator->id << std::endl;
    iterator->endWorker = worker;
  } else {
    //std::cerr << "Iterator can be ended: " << iterator->id << std::endl;
    NanAsyncQueueWorker(worker);
  }

  NanReturnValue(args.Holder());
}

static v8::Persistent<v8::FunctionTemplate> iterator_constructor;

void Iterator::Init () {
  NanScope();

  v8::Local<v8::FunctionTemplate> tpl = NanNew<v8::FunctionTemplate>(Iterator::New);
  NanAssignPersistent(iterator_constructor, tpl);
  tpl->SetClassName(NanNew("Iterator"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  NODE_SET_PROTOTYPE_METHOD(tpl, "next", Iterator::Next);
  NODE_SET_PROTOTYPE_METHOD(tpl, "end", Iterator::End);
}

v8::Handle<v8::Object> Iterator::NewInstance (
        v8::Handle<v8::Object> database
      , v8::Handle<v8::Number> id
      , v8::Handle<v8::Object> optionsObj
    ) {

  NanEscapableScope();

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      NanNew(iterator_constructor);

  if (optionsObj.IsEmpty()) {
    v8::Handle<v8::Value> argv[] = { database, id };
    instance = constructorHandle->GetFunction()->NewInstance(2, argv);
  } else {
    v8::Handle<v8::Value> argv[] = { database, id, optionsObj };
    instance = constructorHandle->GetFunction()->NewInstance(3, argv);
  }

  return NanEscapeScope(instance);
}

NAN_METHOD(Iterator::New) {
  NanScope();

  Database* database = node::ObjectWrap::Unwrap<Database>(args[0]->ToObject());

  //TODO: remove this, it's only here to make NL_STRING_OR_BUFFER_TO_MDVAL happy
  v8::Handle<v8::Function> callback;

  std::string* start = NULL;
  std::string* end = NULL;
  int limit = -1;

  v8::Local<v8::Value> id = args[1];

  v8::Local<v8::Object> optionsObj;

  if (args.Length() > 1 && args[2]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(args[2]);

    if (optionsObj->Has(NanNew("start"))
        && (node::Buffer::HasInstance(optionsObj->Get(NanNew("start")))
          || optionsObj->Get(NanNew("start"))->IsString())) {

      v8::Local<v8::Value> startBuffer =
          NanNew(optionsObj->Get(NanNew("start")));

      // ignore start if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(startBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_start, startBuffer, start)
        start = new std::string((const char*)_start.mv_data, _start.mv_size);
      }
    }

    if (optionsObj->Has(NanNew("end"))
        && (node::Buffer::HasInstance(optionsObj->Get(NanNew("end")))
          || optionsObj->Get(NanNew("end"))->IsString())) {

      v8::Local<v8::Value> endBuffer =
          NanNew(optionsObj->Get(NanNew("end")));

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(endBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_end, endBuffer, end)
        end = new std::string((const char*)_end.mv_data, _end.mv_size);
      }
    }

    if (!optionsObj.IsEmpty() && optionsObj->Has(NanNew("limit"))) {
      limit =
        v8::Local<v8::Integer>::Cast(optionsObj->Get(NanNew("limit")))->Value();
    }
  }

  bool reverse = NanBooleanOptionValue(optionsObj, NanNew("reverse"), false);
  bool keys = NanBooleanOptionValue(optionsObj, NanNew("keys"), true);
  bool values = NanBooleanOptionValue(optionsObj, NanNew("values"), true);
  bool keyAsBuffer = NanBooleanOptionValue(
      optionsObj
    , NanNew("keyAsBuffer")
    , true
  );
  bool valueAsBuffer = NanBooleanOptionValue(
      optionsObj
    , NanNew("valueAsBuffer")
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
  iterator->Wrap(args.This());

  //std::cerr << "New Iterator " << iterator->id << std::endl;

  NanReturnValue(args.This());
}

} // namespace nlmdb

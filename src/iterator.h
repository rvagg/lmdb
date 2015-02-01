/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef NL_ITERATOR_H
#define NL_ITERATOR_H

#include <node.h>
#include <nan.h>

#include "nlmdb.h"
#include "database.h"

namespace nlmdb {

class Database;
class AsyncWorker;

NAN_METHOD(CreateIterator);

class Iterator : public node::ObjectWrap {
public:
  static void Init ();
  static v8::Handle<v8::Object> NewInstance (
      v8::Handle<v8::Object> database
    , v8::Handle<v8::Number> id
    , v8::Handle<v8::Object> optionsObj
  );

  Iterator (
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
    , bool         startIsExclusive
    , bool         endIsExclusive
  );

  ~Iterator ();

  int          Next    (MDB_val *key, MDB_val *value);
  void         End     ();
  void         Release ();

private:
  Database    *database;
  uint32_t     id;
  MDB_txn     *txn;
  MDB_cursor  *cursor;
  std::string *start;
  std::string *end;
  bool         reverse;
  bool         keys;
  bool         values;
  int          limit;
  int          count;

public:
  bool         keyAsBuffer;
  bool         valueAsBuffer;
  bool         startIsExclusive;
  bool         endIsExclusive;
  bool         started;
  bool         nexting;
  bool         ended;
  AsyncWorker* endWorker;

private:
  int          GetIterator ();

  static NAN_METHOD(New);
  static NAN_METHOD(Next);
  static NAN_METHOD(End);
};

} // namespace nlmdb

#endif

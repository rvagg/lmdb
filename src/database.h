/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef NL_DATABASE_H
#define NL_DATABASE_H

#include <map>
#include <vector>
#include <node.h>

#include "nlmdb.h"
#include "iterator.h"

namespace nlmdb {

#define DEFAULT_MAPSIZE 10 << 20 // 10 MB
#define DEFAULT_READERS 126 // LMDB default
#define DEFAULT_SYNC true
#define DEFAULT_READONLY false
#define DEFAULT_WRITEMAP false
#define DEFAULT_METASYNC true
#define DEFAULT_MAPASYNC false
#define DEFAULT_FIXEDMAP false
#define DEFAULT_NOTLS true

typedef struct OpenOptions {
  bool     createIfMissing;
  bool     errorIfExists;
  uint64_t mapSize;
  uint64_t maxReaders;
  bool     sync;
  bool     readOnly;
  bool     writeMap;
  bool     metaSync;
  bool     mapAsync;
  bool     fixedMap;
  bool     notls;
} OpenOptions;

NAN_METHOD(NLMDB);

/* abstract */ class BatchOp {
 public:
  BatchOp (v8::Local<v8::Object> &keyHandle, MDB_val key);
  virtual ~BatchOp ();
  virtual int Execute (MDB_txn *txn, MDB_dbi dbi) =0;

 protected:
  v8::Persistent<v8::Object> persistentHandle;
  MDB_val key;
};

class Database : public node::ObjectWrap {
public:
  static void Init ();
  static v8::Handle<v8::Value> NewInstance (v8::Local<v8::String> &location);

  md_status OpenDatabase (OpenOptions options);
  void CloseDatabase     ();
  int PutToDatabase      (MDB_val key, MDB_val value);
  int PutToDatabase      (std::vector< BatchOp* >* operations);
  int GetFromDatabase    (MDB_val key, MDB_val& value);
  int DeleteFromDatabase (MDB_val key);
  int NewIterator        (MDB_txn **txn, MDB_cursor **cursor);
  void ReleaseIterator   (uint32_t id);
  const char* Location() const;

  Database (const char* location);
  ~Database ();

private:
  MDB_env *env;
  MDB_dbi dbi;

  const char* location;
  uint32_t currentIteratorId;
  void(*pendingCloseWorker);

  std::map< uint32_t, nlmdb::Iterator * > iterators;

  static NAN_METHOD(New);
  static NAN_METHOD(Open);
  static NAN_METHOD(Close);
  static NAN_METHOD(Put);
  static NAN_METHOD(Get);
  static NAN_METHOD(Delete);
  static NAN_METHOD(Batch);
  static NAN_METHOD(Iterator);
};

} // namespace nlmdb

#endif

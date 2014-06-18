/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include <node.h>
#include <sys/stat.h>
#include <stdlib.h> // TODO: remove when we remove system()
#include <nan.h>

#include "database.h"
#include "database_async.h"
#include "batch.h"
#include "iterator.h"

#include <string.h>

namespace nlmdb {

#if (NODE_MODULE_VERSION > 0x000B)
  typedef uv_stat_t * __uv_stat__;
#else
  typedef uv_statbuf_t * __uv_stat__;
#endif

inline __uv_stat__ Stat (const char* path) {
  uv_fs_t req;
  int result = uv_fs_lstat(uv_default_loop(), &req, path, NULL);
  if (result < 0)
    return NULL;
  return static_cast<const __uv_stat__>(req.ptr);
}

inline bool IsDirectory (const __uv_stat__ stat) {
  return (stat->st_mode & S_IFMT) == S_IFDIR;
}

inline bool MakeDirectory (const char* path) {
  uv_fs_t req;
  return uv_fs_mkdir(uv_default_loop(), &req, path, 511, NULL);
}

Database::Database (const char* location) : location(location) {
  //dbi = NULL;
  currentIteratorId = 0;
  pendingCloseWorker = NULL;
};

Database::~Database () {
  //if (dbi != NULL)
  //  delete dbi;
  delete[] location;
};

const char* Database::Location() const {
  return location;
}

void Database::ReleaseIterator (uint32_t id) {
  // called each time an Iterator is End()ed, in the main thread
  // we have to remove our reference to it and if it's the last iterator
  // we have to invoke a pending CloseWorker if there is one
  // if there is a pending CloseWorker it means that we're waiting for
  // iterators to end before we can close them
  iterators.erase(id);
  //std::cerr << "ReleaseIterator: " << iterators.size() << std::endl;
  if (iterators.size() == 0 && pendingCloseWorker != NULL) {
    //std::cerr << "pendingCloseWorker RUNNING\n";
    NanAsyncQueueWorker((NanAsyncWorker*)pendingCloseWorker);
    pendingCloseWorker = NULL;
  }
}

md_status Database::OpenDatabase (OpenOptions options) {
  md_status status;

  // Emulate the behaviour of LevelDB create_if_missing & error_if_exists
  // options, with an additional check for stat == directory
  const __uv_stat__ stat = Stat(location);
  if (stat == NULL) {
    if (options.createIfMissing) {
      status.code = MakeDirectory(location);
      if (status.code)
        return status;
    } else {
      status.error = std::string(location);
      status.error += " does not exist (createIfMissing is false)";
      return status;
    }
  } else {
    if (!IsDirectory(stat)) {
      status.error = std::string(location);
      status.error += " exists and is not a directory";
      return status;
    }
    if (options.errorIfExists) {
      status.error = std::string(location);
      status.error += " exists (errorIfExists is true)";
      return status;
    }
  }

  int env_opt = 0;
  if (!options.sync)
    env_opt |= MDB_NOSYNC;
  if (options.readOnly)
    env_opt |= MDB_RDONLY;
  if (options.writeMap)
    env_opt |= MDB_WRITEMAP;
  if (!options.metaSync)
    env_opt |= MDB_NOMETASYNC;
  if (options.mapAsync)
    env_opt |= MDB_MAPASYNC;
  if (options.fixedMap)
    env_opt |= MDB_FIXEDMAP;
  if (!options.notls)
    env_opt |= MDB_NOTLS;

  status.code = mdb_env_create(&env);
  if (status.code)
    return status;

  status.code = mdb_env_set_mapsize(env, options.mapSize);
  if (status.code)
    return status;

  status.code = mdb_env_set_maxreaders(env, options.maxReaders);
  if (status.code)
    return status;

  //TODO: yuk
  if (options.createIfMissing) {
    char cmd[200];
    sprintf(cmd, "mkdir -p %s", location);
    status.code = system(cmd);
    if (status.code)
      return status;
  }

  status.code = mdb_env_open(env, location, env_opt, 0664);
  return status;
}

void Database::CloseDatabase () {
  mdb_env_close(env);
}

int Database::PutToDatabase (MDB_val key, MDB_val value) {
  int rc;
  MDB_txn *txn;

  //std::cerr << "PUTTODB(" << (char*)key.mv_data << "(" << key.mv_size << ")," << (char*)value.mv_data << "(" << value.mv_size << "))" << std::endl;

  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc)
    return rc;
  rc = mdb_open(txn, NULL, 0, &dbi);
  if (rc) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = mdb_put(txn, dbi, &key, &value, 0);
  if (rc) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = mdb_txn_commit(txn);
  //std::cerr << "FINISHED PUTTODB: " << rc << ", " << value.mv_size << std::endl;
  return rc;
}

int Database::PutToDatabase (std::vector< BatchOp* >* operations) {
  int rc;
  MDB_txn *txn;

  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc)
    return rc;
  rc = mdb_open(txn, NULL, 0, &dbi);
  if (rc) {
    mdb_txn_abort(txn);
    return rc;
  }

  for (std::vector< BatchOp* >::iterator it = operations->begin()
      ; it != operations->end()
      ; it++) {

    //printf("BatchOp.Execute()\n");fflush(stdout);
    rc = (*it)->Execute(txn, dbi);
    if (rc) {
      mdb_txn_abort(txn);
      return rc;
    }
  }

  rc = mdb_txn_commit(txn);

  return rc;
}

int Database::GetFromDatabase (MDB_val key, MDB_val& value) {
  int rc;
  MDB_txn *txn;

  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc)
    return rc;
  rc = mdb_open(txn, NULL, 0, &dbi);
  if (rc) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = mdb_get(txn, dbi, &key, &value);
  if (rc) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = mdb_txn_commit(txn);

  //std::cerr << rc << " GETFROMDB(" << (char*)key.mv_data << "(" << key.mv_size << ")," << std::string((char*)value.mv_data, value.mv_size) << "))" << std::endl;

  return rc;
}

int Database::DeleteFromDatabase (MDB_val key) {
  int rc;
  MDB_txn *txn;

  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc)
    return rc;
  rc = mdb_open(txn, NULL, 0, &dbi);
  if (rc)
    return rc;
  rc = mdb_del(txn, dbi, &key, NULL);
  if (rc)
    return rc;
  rc = mdb_txn_commit(txn);
  return rc;
}

int Database::NewIterator (MDB_txn **txn, MDB_cursor **cursor) {
  int rc;

  //std::cerr << "opening transaction..." << std::endl;
  rc = mdb_txn_begin(env, NULL, MDB_RDONLY, txn);
  //std::cerr << "opened transaction! " << cursor << ", " << strerror(rc) << std::endl;
  if (rc)
    return rc;
  rc = mdb_open(*txn, NULL, 0, &dbi);
  if (rc) {
    mdb_txn_abort(*txn);
    return rc;
  }
  rc = mdb_cursor_open(*txn, dbi, cursor);
  
  //std::cerr << "opened cursor! " << cursor << ", " << strerror(rc) << std::endl;
  return rc;
}

static v8::Persistent<v8::FunctionTemplate> database_constructor;

NAN_METHOD(NLMDB) {
  NanScope();

  v8::Local<v8::String> location;
  if (args.Length() != 0 && args[0]->IsString())
    location = args[0].As<v8::String>();
  NanReturnValue(Database::NewInstance(location));
}

void Database::Init () {
  NanScope();

  v8::Local<v8::FunctionTemplate> tpl = NanNew<v8::FunctionTemplate>(Database::New);
  NanAssignPersistent(database_constructor, tpl);
  tpl->SetClassName(NanNew("Database"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  NODE_SET_PROTOTYPE_METHOD(tpl, "open", Database::Open);
  NODE_SET_PROTOTYPE_METHOD(tpl, "close", Database::Close);
  NODE_SET_PROTOTYPE_METHOD(tpl, "put", Database::Put);
  NODE_SET_PROTOTYPE_METHOD(tpl, "get", Database::Get);
  NODE_SET_PROTOTYPE_METHOD(tpl, "del", Database::Delete);
  NODE_SET_PROTOTYPE_METHOD(tpl, "batch", Database::Batch);
  NODE_SET_PROTOTYPE_METHOD(tpl, "iterator", Database::Iterator);
}

NAN_METHOD(Database::New) {
  NanScope();

  if (args.Length() == 0) {
    return NanThrowError("constructor requires at least a location argument");
  }

  if (!args[0]->IsString()) {
    return NanThrowError("constructor requires a location string argument");
  }

  char* location = FromV8String(args[0]);

  Database* obj = new Database(location);
  obj->Wrap(args.This());

  NanReturnValue(args.This());
}

v8::Handle<v8::Value> Database::NewInstance (v8::Local<v8::String> &location) {
  NanEscapableScope();

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      NanNew(database_constructor);

  if (location.IsEmpty()) {
    instance = constructorHandle->GetFunction()->NewInstance(0, NULL);
  } else {
    v8::Handle<v8::Value> argv[] = { location };
    instance = constructorHandle->GetFunction()->NewInstance(1, argv);
  }

  return NanEscapeScope(instance);
}

NAN_METHOD(Database::Open) {
  NanScope();

  NL_METHOD_SETUP_COMMON(open, 0, 1)

  OpenOptions options;

  options.createIfMissing = NanBooleanOptionValue(
      optionsObj
    , NanNew("createIfMissing")
    , true
  );
  options.errorIfExists = NanBooleanOptionValue(
      optionsObj
    , NanNew("errorIfExists")
  , false);
  options.mapSize = UInt64OptionValue(
      optionsObj
    , NanNew("mapSize")
    , DEFAULT_MAPSIZE
  );
  options.maxReaders = UInt64OptionValue(
      optionsObj
    , NanNew("maxReaders")
    , DEFAULT_READERS
  );
  options.sync = NanBooleanOptionValue(
      optionsObj
    , NanNew("sync")
  , DEFAULT_SYNC);
  options.readOnly = NanBooleanOptionValue(
      optionsObj
    , NanNew("readOnly")
    , DEFAULT_READONLY
  );
  options.writeMap = NanBooleanOptionValue(
      optionsObj
    , NanNew("writeMap")
    , DEFAULT_READONLY
  );
  options.metaSync = NanBooleanOptionValue(
      optionsObj
    , NanNew("metaSync")
    , DEFAULT_METASYNC
  );
  options.mapAsync = NanBooleanOptionValue(
      optionsObj
    , NanNew("mapAsync")
    , DEFAULT_MAPASYNC
  );
  options.fixedMap = NanBooleanOptionValue(
      optionsObj
    , NanNew("fixedMap")
    , DEFAULT_FIXEDMAP
  );
  options.metaSync = NanBooleanOptionValue(
      optionsObj
    , NanNew("notls")
    , DEFAULT_NOTLS
  );

  OpenWorker* worker = new OpenWorker(
      database
    , new NanCallback(callback)
    , options
  );

  // protect against accidental GC
  v8::Local<v8::Object> _this = args.This();
  worker->SaveToPersistent("database", _this);

  NanAsyncQueueWorker(worker);

  NanReturnUndefined();
}

NAN_METHOD(Database::Close) {
  NanScope();

  NL_METHOD_SETUP_COMMON_ONEARG(close)

  CloseWorker* worker = new CloseWorker(
      database
    , new NanCallback(callback)
  );

  if (database->iterators.size() > 0) {
    // yikes, we still have iterators open! naughty naughty.
    // we have to queue up a CloseWorker and manually close each of them.
    // the CloseWorker will be invoked once they are all cleaned up
    database->pendingCloseWorker = worker;

    for (
        std::map< uint32_t, nlmdb::Iterator * >::iterator it
            = database->iterators.begin()
      ; it != database->iterators.end()
      ; ++it) {

        // for each iterator still open, first check if it's already in
        // the process of ending (ended==true means an async End() is
        // in progress), if not, then we call End() with an empty callback
        // function and wait for it to hit ReleaseIterator() where our
        // CloseWorker will be invoked

        nlmdb::Iterator *iterator = it->second;

        if (!iterator->ended) {
          v8::Local<v8::Function> end =
              NanObjectWrapHandle(iterator)
                ->Get(NanNew("end"))
                  .As<v8::Function>();
          v8::Local<v8::Value> argv[] = {
              NanNew<v8::FunctionTemplate>()->GetFunction() // empty callback
          };
          v8::TryCatch try_catch;
          end->Call(NanObjectWrapHandle(iterator), 1, argv);
          if (try_catch.HasCaught()) {
            node::FatalException(try_catch);
          }
        }
    }
  } else {
    // protect against accidental GC
    v8::Local<v8::Object> _this = args.This();
    worker->SaveToPersistent("database", _this);

    NanAsyncQueueWorker(worker);
  }

  NanReturnUndefined();
}

NAN_METHOD(Database::Put) {
  NanScope();

  NL_METHOD_SETUP_COMMON(put, 2, 3)

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[0], key)
  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[1], value)

  v8::Local<v8::Object> keyHandle = args[0].As<v8::Object>();
  v8::Local<v8::Object> valueHandle = args[1].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)
  NL_STRING_OR_BUFFER_TO_MDVAL(value, valueHandle, value)

  //std::cerr << "->PUTTODB(" << (char*)key.mv_data << "(" << key.mv_size << ")," << (char*)value.mv_data << "(" << value.mv_size << "))" << std::endl;

  WriteWorker* worker  = new WriteWorker(
      database
    , new NanCallback(callback)
    , key
    , value
    , keyHandle
    , valueHandle
  );

  // protect against accidental GC
  v8::Local<v8::Object> _this = args.This();
  worker->SaveToPersistent("database", _this);

  NanAsyncQueueWorker(worker);

  NanReturnUndefined();
}

NAN_METHOD(Database::Get) {
  NanScope();

  NL_METHOD_SETUP_COMMON(get, 1, 2)

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[0], key)

  v8::Local<v8::Object> keyHandle = args[0].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)

  //std::cerr << "->GETFROMDB(" << (char*)key.mv_data << "(" << key.mv_size << ")" << std::endl;

  bool asBuffer = NanBooleanOptionValue(optionsObj, NanNew("asBuffer"), true);

  ReadWorker* worker = new ReadWorker(
      database
    , new NanCallback(callback)
    , key
    , asBuffer
    , keyHandle
  );

  // protect against accidental GC
  v8::Local<v8::Object> _this = args.This();
  worker->SaveToPersistent("database", _this);

  NanAsyncQueueWorker(worker);

  NanReturnUndefined();
}

NAN_METHOD(Database::Delete) {
  NanScope();

  NL_METHOD_SETUP_COMMON(del, 1, 2)

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(args[0], key)

  v8::Local<v8::Object> keyHandle = args[0].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)

  DeleteWorker* worker = new DeleteWorker(
      database
    , new NanCallback(callback)
    , key
    , keyHandle
  );

  // protect against accidental GC
  v8::Local<v8::Object> _this = args.This();
  worker->SaveToPersistent("database", _this);

  NanAsyncQueueWorker(worker);

  NanReturnUndefined();
}

NAN_METHOD(Database::Batch) {
  NanScope();

  if ((args.Length() == 0 || args.Length() == 1) && !args[0]->IsArray()) {
    v8::Local<v8::Object> optionsObj;
    if (args.Length() > 0 && args[0]->IsObject()) {
      optionsObj = v8::Local<v8::Object>::Cast(args[0]);
    }
    NanReturnValue(WriteBatch::NewInstance(args.This(), optionsObj));
  }

  NL_METHOD_SETUP_COMMON(batch, 1, 2)

  v8::Local<v8::Array> array = v8::Local<v8::Array>::Cast(args[0]);
  WriteBatch* batch = new WriteBatch(database);

  for (unsigned int i = 0; i < array->Length(); i++) {
    if (!array->Get(i)->IsObject())
      continue;

    v8::Local<v8::Object> obj = v8::Local<v8::Object>::Cast(array->Get(i));

    NL_CB_ERR_IF_NULL_OR_UNDEFINED(obj->Get(NanNew("type")), type)

    v8::Local<v8::Object> keyBuffer = obj->Get(NanNew("key")).As<v8::Object>();
    NL_CB_ERR_IF_NULL_OR_UNDEFINED(keyBuffer, key)

    if (obj->Get(NanNew("type"))->StrictEquals(NanNew("del"))) {
      NL_STRING_OR_BUFFER_TO_MDVAL(key, keyBuffer, key)
      batch->Delete(keyBuffer, key);
    } else if (obj->Get(NanNew("type"))->StrictEquals(NanNew("put"))) {
      v8::Local<v8::Object> valueBuffer = obj->Get(NanNew("value")).As<v8::Object>();
      NL_CB_ERR_IF_NULL_OR_UNDEFINED(valueBuffer, value)
      NL_STRING_OR_BUFFER_TO_MDVAL(key, keyBuffer, key)
      NL_STRING_OR_BUFFER_TO_MDVAL(value, valueBuffer, value)

      batch->Put(keyBuffer, key, valueBuffer, value);
    }
  }

  batch->Write(callback);

  NanReturnUndefined();
}

NAN_METHOD(Database::Iterator) {
  NanScope();

  Database* database = node::ObjectWrap::Unwrap<Database>(args.This());

  v8::Local<v8::Object> optionsObj;
  if (args.Length() > 0 && args[0]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(args[0]);
  }

  // each iterator gets a unique id for this Database, so we can
  // easily store & lookup on our `iterators` map
  uint32_t id = database->currentIteratorId++;
  v8::TryCatch try_catch;
  v8::Handle<v8::Object> iteratorHandle = Iterator::NewInstance(
      args.This()
    , NanNew(id)
    , optionsObj
  );
  if (try_catch.HasCaught()) {
    node::FatalException(try_catch);
  }

  nlmdb::Iterator *iterator =
      node::ObjectWrap::Unwrap<nlmdb::Iterator>(iteratorHandle);

  // register our iterator
  database->iterators[id] = iterator;

  NanReturnValue(iteratorHandle);
}

} // namespace nlmdb

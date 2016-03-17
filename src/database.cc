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
    Nan::AsyncQueueWorker((Nan::AsyncWorker*)pendingCloseWorker);
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

static Nan::Persistent<v8::FunctionTemplate> database_constructor;

NAN_METHOD(NLMDB) {
  Nan::HandleScope scope;

  v8::Local<v8::String> location;
  if (info.Length() != 0 && info[0]->IsString())
    location = info[0].As<v8::String>();
  info.GetReturnValue().Set(Database::NewInstance(location));
}

void Database::Init () {
  Nan::HandleScope scope;

  v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(Database::New);
  database_constructor.Reset(tpl);
  tpl->SetClassName(Nan::New("Database").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);
  Nan::SetPrototypeMethod(tpl, "open", Database::Open);
  Nan::SetPrototypeMethod(tpl, "close", Database::Close);
  Nan::SetPrototypeMethod(tpl, "put", Database::Put);
  Nan::SetPrototypeMethod(tpl, "get", Database::Get);
  Nan::SetPrototypeMethod(tpl, "del", Database::Delete);
  Nan::SetPrototypeMethod(tpl, "batch", Database::Batch);
  Nan::SetPrototypeMethod(tpl, "iterator", Database::Iterator);
}

NAN_METHOD(Database::New) {
  Nan::HandleScope scope;

  if (info.Length() == 0) {
    return Nan::ThrowError("constructor requires at least a location argument");
  }

  if (!info[0]->IsString()) {
    return Nan::ThrowError("constructor requires a location string argument");
  }

  char* location = FromV8String(info[0]);

  Database* obj = new Database(location);
  obj->Wrap(info.This());

  info.GetReturnValue().Set(info.This());
}

v8::Handle<v8::Value> Database::NewInstance (v8::Local<v8::String> &location) {
  Nan::HandleScope scope;

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      Nan::New(database_constructor);

  if (location.IsEmpty()) {
    instance = constructorHandle->GetFunction()->NewInstance(0, NULL);
  } else {
    v8::Handle<v8::Value> argv[] = { location };
    instance = constructorHandle->GetFunction()->NewInstance(1, argv);
  }

  return instance;
}

NAN_METHOD(Database::Open) {
  Nan::HandleScope scope;

  NL_METHOD_SETUP_COMMON(open, 0, 1)

  OpenOptions options;

  options.createIfMissing = BooleanOptionValueDef(
      optionsObj
    , Nan::New("createIfMissing").ToLocalChecked()
    , true
  );
  options.errorIfExists = BooleanOptionValueDef(
      optionsObj
    , Nan::New("errorIfExists").ToLocalChecked()
  , false);
  options.mapSize = UInt64OptionValue(
      optionsObj
    , Nan::New("mapSize").ToLocalChecked()
    , DEFAULT_MAPSIZE
  );
  options.maxReaders = UInt64OptionValue(
      optionsObj
    , Nan::New("maxReaders").ToLocalChecked()
    , DEFAULT_READERS
  );
  options.sync = BooleanOptionValueDef(
      optionsObj
    , Nan::New("sync").ToLocalChecked()
  , DEFAULT_SYNC);
  options.readOnly = BooleanOptionValueDef(
      optionsObj
    , Nan::New("readOnly").ToLocalChecked()
    , DEFAULT_READONLY
  );
  options.writeMap = BooleanOptionValueDef(
      optionsObj
    , Nan::New("writeMap").ToLocalChecked()
    , DEFAULT_READONLY
  );
  options.metaSync = BooleanOptionValueDef(
      optionsObj
    , Nan::New("metaSync").ToLocalChecked()
    , DEFAULT_METASYNC
  );
  options.mapAsync = BooleanOptionValueDef(
      optionsObj
    , Nan::New("mapAsync").ToLocalChecked()
    , DEFAULT_MAPASYNC
  );
  options.fixedMap = BooleanOptionValueDef(
      optionsObj
    , Nan::New("fixedMap").ToLocalChecked()
    , DEFAULT_FIXEDMAP
  );
  options.metaSync = BooleanOptionValueDef(
      optionsObj
    , Nan::New("notls").ToLocalChecked()
    , DEFAULT_NOTLS
  );

  OpenWorker* worker = new OpenWorker(
      database
    , new Nan::Callback(callback)
    , options
  );

  Nan::AsyncQueueWorker(worker);

  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(Database::Close) {
  Nan::HandleScope scope;

  NL_METHOD_SETUP_COMMON_ONEARG(close)

  CloseWorker* worker = new CloseWorker(
      database
    , new Nan::Callback(callback)
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
              iterator->handle()
                ->Get(Nan::New("end").ToLocalChecked())
                  .As<v8::Function>();
          v8::Local<v8::Value> argv[] = {
              Nan::GetFunction(Nan::New<v8::FunctionTemplate>()).ToLocalChecked() // empty callback
          };
          v8::TryCatch try_catch;
          Nan::Callback(end).Call(iterator->handle(), 1, argv);
          if (try_catch.HasCaught()) {
            node::FatalException(try_catch);
          }
        }
    }
  } else {
    Nan::AsyncQueueWorker(worker);
  }

  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(Database::Put) {
  Nan::HandleScope scope;

  NL_METHOD_SETUP_COMMON(put, 2, 3)

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[0], key)
  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[1], value)

  v8::Local<v8::Object> keyHandle = info[0].As<v8::Object>();
  v8::Local<v8::Object> valueHandle = info[1].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)
  NL_STRING_OR_BUFFER_TO_MDVAL(value, valueHandle, value)

  //std::cerr << "->PUTTODB(" << (char*)key.mv_data << "(" << key.mv_size << ")," << (char*)value.mv_data << "(" << value.mv_size << "))" << std::endl;

  WriteWorker* worker  = new WriteWorker(
      database
    , new Nan::Callback(callback)
    , key
    , value
    , keyHandle
    , valueHandle
  );
  Nan::AsyncQueueWorker(worker);

  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(Database::Get) {
  Nan::HandleScope scope;

  NL_METHOD_SETUP_COMMON(get, 1, 2)

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[0], key)

  v8::Local<v8::Object> keyHandle = info[0].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)

  //std::cerr << "->GETFROMDB(" << (char*)key.mv_data << "(" << key.mv_size << ")" << std::endl;

  bool asBuffer = BooleanOptionValueDef(optionsObj, Nan::New("asBuffer").ToLocalChecked(), true);

  ReadWorker* worker = new ReadWorker(
      database
    , new Nan::Callback(callback)
    , key
    , asBuffer
    , keyHandle
  );
  Nan::AsyncQueueWorker(worker);

  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(Database::Delete) {
  Nan::HandleScope scope;

  NL_METHOD_SETUP_COMMON(del, 1, 2)

  NL_CB_ERR_IF_NULL_OR_UNDEFINED(info[0], key)

  v8::Local<v8::Object> keyHandle = info[0].As<v8::Object>();
  NL_STRING_OR_BUFFER_TO_MDVAL(key, keyHandle, key)

  DeleteWorker* worker = new DeleteWorker(
      database
    , new Nan::Callback(callback)
    , key
    , keyHandle
  );
  Nan::AsyncQueueWorker(worker);

  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(Database::Batch) {
  Nan::HandleScope scope;

  if ((info.Length() == 0 || info.Length() == 1) && !info[0]->IsArray()) {
    v8::Local<v8::Object> optionsObj;
    if (info.Length() > 0 && info[0]->IsObject()) {
      optionsObj = v8::Local<v8::Object>::Cast(info[0]);
    }
    info.GetReturnValue().Set(WriteBatch::NewInstance(info.This(), optionsObj));
  }

  NL_METHOD_SETUP_COMMON(batch, 1, 2)

  v8::Local<v8::Array> array = v8::Local<v8::Array>::Cast(info[0]);
  WriteBatch* batch = new WriteBatch(database);

  for (unsigned int i = 0; i < array->Length(); i++) {
    if (!array->Get(i)->IsObject())
      continue;

    v8::Local<v8::Object> obj = v8::Local<v8::Object>::Cast(array->Get(i));

    NL_CB_ERR_IF_NULL_OR_UNDEFINED(obj->Get(Nan::New("type").ToLocalChecked()), type)

    v8::Local<v8::Object> keyBuffer = obj->Get(Nan::New("key").ToLocalChecked()).As<v8::Object>();
    NL_CB_ERR_IF_NULL_OR_UNDEFINED(keyBuffer, key)

    if (obj->Get(Nan::New("type").ToLocalChecked())->StrictEquals(Nan::New("del").ToLocalChecked())) {
      NL_STRING_OR_BUFFER_TO_MDVAL(key, keyBuffer, key)
      batch->Delete(keyBuffer, key);
    } else if (obj->Get(Nan::New("type").ToLocalChecked())->StrictEquals(Nan::New("put").ToLocalChecked())) {
      v8::Local<v8::Object> valueBuffer = obj->Get(Nan::New("value").ToLocalChecked()).As<v8::Object>();
      NL_CB_ERR_IF_NULL_OR_UNDEFINED(valueBuffer, value)
      NL_STRING_OR_BUFFER_TO_MDVAL(key, keyBuffer, key)
      NL_STRING_OR_BUFFER_TO_MDVAL(value, valueBuffer, value)

      batch->Put(keyBuffer, key, valueBuffer, value);
    }
  }

  batch->Write(callback);

  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(Database::Iterator) {
  Nan::HandleScope scope;

  Database* database = Nan::ObjectWrap::Unwrap<Database>(info.This());

  v8::Local<v8::Object> optionsObj;
  if (info.Length() > 0 && info[0]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(info[0]);
  }

  // each iterator gets a unique id for this Database, so we can
  // easily store & lookup on our `iterators` map
  uint32_t id = database->currentIteratorId++;
  v8::TryCatch try_catch;
  v8::Handle<v8::Object> iteratorHandle = Iterator::NewInstance(
      info.This()
    , Nan::New<v8::Number>(id)
    , optionsObj
  );
  if (try_catch.HasCaught()) {
    node::FatalException(try_catch);
  }

  nlmdb::Iterator *iterator =
      Nan::ObjectWrap::Unwrap<nlmdb::Iterator>(iteratorHandle);

  // register our iterator
  database->iterators[id] = iterator;

  info.GetReturnValue().Set(iteratorHandle);
}

} // namespace nlmdb

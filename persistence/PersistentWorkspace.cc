/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "persistence/PersistentWorkspace.hh"

#include <fcntl.h>

#include "persistence/Unserializer.hh"

namespace mica {

using std::vector;

#define CACHE_WIDTH 64
#define CACHE_GROW_WINDOW 32

boost::tuple<WID, Var> PersistentWorkspace::open(const Symbol &name, const Ref<Object> &parent_lobby) {
  PersistentWorkspace *pool = new PersistentWorkspace(name);
  pool->wid_ = Workspaces::instance.add(name, pool);

  pool->initialize();

  /** Lobby is always the first object in the pool.  Try to get it.
   *  If this fails, create it.
   */
  if (!pool->exists(0)) {
    pool->lobby_ = Object::create(pool->wid_, parent_lobby)->asRef<Object>();

  } else {
    /** If there's a lobby, there's a task db, too.
     */
    pool->load_tasks();

    pool->lobby_ = pool->resolve(0);
  }

  return boost::tuple<WID, Var>(pool->wid_, Var(pool->lobby_));
}

PersistentWorkspace::PersistentWorkspace(const Symbol &poolName)
    : Workspace(poolName), cache_width(CACHE_WIDTH), cache_grow_window(CACHE_GROW_WINDOW) {
  for (int i = 0; i < NUM_DBS; i++) {
    int rc;
    if ((rc = mdb_env_create(&db_env_[i])) != 0) {
      throw internal_error("unable to initialize database environment");
    }
  }
}

void PersistentWorkspace::initialize() {
  /** Set name for each DB
   */
  mica_string basename = pool_name_.tostring();
  for (int i = 0; i < NUM_DBS; i++) names_[i] = basename;

  names_[ENV_DB].append(".env");
  names_[OID_DB].append(".oid");

  /** Open each DB file
   */

  for (int i = 0; i < NUM_DBS; i++) {
    if (mdb_env_open(db_env_[i], names_[i].c_str(), 0, 0644) != 0) {
      throw internal_error("cannot open db environment");
    }

    if (mdb_txn_begin(db_env_[i], nullptr, 0, &db_txn_[i]) != 0) {
      throw internal_error("cannot open db transaction");
    }

    if (mdb_open(db_txn_[i], nullptr, 0, &db_dbi_[i]) != 0) {
      throw internal_error("cannot open db");
    }
  }
}

void PersistentWorkspace::sync() {
  this->Workspace::sync();

  // First we do a cache cleanup.

  // First pass,
  for (vector<ObjectEntry *>::iterator oe = objects.begin(); oe != objects.end(); oe++) {
  }

  // Write all object handles and environments to disk
  for (vector<ObjectEntry *>::iterator oe = objects.begin(); oe != objects.end(); oe++) {
    if (*oe) {
      OID oid = oe - objects.begin();

      ObjectEntry *entry = *oe;
      if (entry->object)
        write_object(oid);
      if (entry->environment)
        write(oid);
    }
  }

  for (int i = 0; i < NUM_DBS; i++) {
    mdb_env_sync(db_env_[i], 0);
  }
}

void PersistentWorkspace::close() {
  /** Save the task list for the next open session
   */
  save_tasks();

  sync();

  for (int i = 0; i < NUM_DBS; i++) {
    mdb_close(db_env_[i], db_dbi_[i]);
    mdb_env_close(db_env_[i]);
  }

  this->Workspace::close();
}

void PersistentWorkspace::del(OID idx) {
  MDB_val key;
  key.mv_data = &idx;
  key.mv_size = sizeof(idx);

  MDB_txn *txn_oid, *txn_env;
  if (mdb_txn_begin(db_env_[OID_DB], nullptr, 0, &txn_oid) !=0) {
    throw internal_error("unable to start delete transaction in oid db");
  }
  if (mdb_del(txn_oid, db_dbi_[OID_DB], &key, nullptr) !=0 ){
    mdb_txn_abort(txn_oid);
    throw internal_error("unable to delete key from oid db");
  }
  if (mdb_txn_begin(db_env_[ENV_DB], nullptr, 0, &txn_env) !=0) {
    throw internal_error("unable to start delete transaction in env db");
  }
  if (mdb_del(txn_env, db_dbi_[ENV_DB], &key, nullptr) !=0 ) {
    mdb_txn_abort(txn_env);
    throw internal_error("unable to delete key from env db");
  }
  if (mdb_txn_commit(txn_oid) !=0 ) {
    throw internal_error("unable to commit delete transaction in oid db");
  }
  if (mdb_txn_commit(txn_env) !=0 ) {
    throw internal_error("unable to commit delete transaction in env db");
  }

  /** Turf the cache entry for this object -- just reduce the use count
   *  to 0 and set the "deleted" flag.  It will be removed at the
   *  next cache flush.
   */
  int cache_id;
  if ((cache_id = objects[idx]->cache_id) != -1)
    cache_list_[cache_id].deleted = true;

  this->Workspace::del(idx);
}

inline void file_error() {
  char errmsg[80];
  snprintf(errmsg, 80, "error reading task file: %s", strerror(errno));
  throw internal_error(errmsg);
}

void PersistentWorkspace::load_tasks() {
  notify_start_paging();

  /** Open the task file - read only.
   */
  mica_string task_fname(pool_name_.tostring());
  task_fname.append(".tsk");
  int flags = O_RDONLY;
  int task_fd = ::open(task_fname.c_str(), flags, S_IRWXU);

  /** Now iterate through it and retrieve each task
   */
  while (1) {
    size_t serialized_size;
    int error = read(task_fd, &serialized_size, sizeof(serialized_size));
    if (error == 0)
      break;
    else if (error == -1) {
      file_error();
    }

    char buffer[serialized_size];
    error = read(task_fd, buffer, serialized_size);
    if (error != boost::numeric_cast<int>(serialized_size)) {
      throw internal_error("unable to read task from task list");
    } else if (error == -1) {
      file_error();
    }

    mica_string buffer_string(buffer, serialized_size);
    Unserializer unserializer(buffer_string);

    Ref<Task> task(unserializer.parseTaskReal());

    if (task->tid >= managed_tasks_.size())
      ;
    managed_tasks_.resize(task->tid + 64);

    managed_tasks_[task->tid] = new TaskEntry((Task *)task, task->tid);
  }
  ::close(task_fd);

  notify_end_paging();

  /** Now put all the blank spots in the task list into the free
   *  list.
   */
  for (vector<TaskEntry *>::reverse_iterator f = managed_tasks_.rbegin(); f != managed_tasks_.rend();
       f++) {
    if (!(*f)) {
      free_task_list_.push_back((managed_tasks_.rend() - f) - 1);
    }
  }
}

void PersistentWorkspace::save_tasks() {
  reference_counted::collect_cycles();

  notify_start_paging();

  /** Open the task file - write, truncate.
   */
  mica_string task_fname(pool_name_.tostring());
  task_fname.append(".tsk");
  int flags = O_WRONLY | O_CREAT | O_TRUNC;
  int task_fd = ::open(task_fname.c_str(), flags, S_IRWXU);

  /** Now for each task in managed_tasks, write...
   */
  for (vector<TaskEntry *>::iterator ti = managed_tasks_.begin(); ti != managed_tasks_.end(); ti++) {
    if (*ti) {
      Task *task = (*ti)->task;

      serialize_buffer buffer;
      task->serialize_full_to(buffer);

      /** Write size first.
       */
      size_t buffer_size = buffer.size();

      ::write(task_fd, &buffer_size, sizeof(buffer_size));

      /** Now write the string
       */
      ::write(task_fd, buffer.c_str(), buffer_size);
    }
  }

  ::close(task_fd);

  notify_end_paging();
}

void PersistentWorkspace::push_cache(OID oid) {
  /** Pushes a new object to the cache.  Before doing so, we free up
   *  anything whose usecnt isn't up to spec.
   *  We don't flush unless the cache is full to the point of
   *  being bigger than the max cache_width PLUS the grow window
   *  We never flush the cache while collecting cycles.
   */
  if (!cycle_collecting() && cache_list_.size() > (cache_width + cache_grow_window))
    flush_cache();

  /** Now push the object onto the end of the cache
   */
  unsigned int cache_id = cache_list_.size();
  cache_list_.push_back(CacheEntry(oid, 1));
  objects[oid]->cache_id = cache_id;
}

void PersistentWorkspace::flush_cache() {
  // Sort the cache_list in descending order.  This puts objects with
  // the lowest use count at the end
  // O(N Log N) on average.  Maybe better to try and use nth_element (O(N))
  sort(cache_list_.begin(), cache_list_.end());

  // Count cache_width elements in -- anything after that is expired
  // and everything before it is decremented by the usecnt of that element
  CacheVector::iterator last_valid = cache_list_.begin() + cache_width;
  unsigned int lowest_use_cnt = last_valid->usecnt;

  // Now we go through and expire everything after it
  for (CacheVector::iterator expire = last_valid + 1; expire != cache_list_.end(); expire++) {
    if (!expire->deleted && objects[expire->object_id] && objects[expire->object_id]->object) {
      write_object(expire->object_id);

      // Sync and then turf the environment.  But only if there is one.
      if (objects[expire->object_id]->environment) {
        // PAGING MODE ON (paging environment out of cache)
        notify_start_paging();
        write(expire->object_id);
        delete objects[expire->object_id]->environment;

        objects[expire->object_id]->environment = 0;
        objects[expire->object_id]->cache_id = -1;
        notify_end_paging();
        // PAGING MODE OFF
      }
    }
  }

  // Clear out the entries just expired:
  cache_list_.resize(cache_width);

  // Now go through and do two things:
  //    subtract the lowest_use_cnt from each use count
  //    reset the object's cache entry id to its new location
  for (CacheVector::iterator renum = cache_list_.begin(); renum != cache_list_.end(); renum++) {
    renum->usecnt -= lowest_use_cnt;

    int c_id = renum - cache_list_.begin();
    OID oid = renum->object_id;
    if (!renum->deleted && objects[oid])
      objects[oid]->cache_id = c_id;
  }

  assert(objects[cache_list_[0].object_id]->cache_id == 0);
}

OStorage *PersistentWorkspace::get_environment(OID object_id) {
  /** CACHE HIT
   */
  if (objects[object_id]->environment) {
    int cache_id = objects[object_id]->cache_id;

    /** If it has no cache entry, then push it to cache!
     */
    if (cache_id == -1)
      push_cache(object_id);
    else
      cache_list_[cache_id].usecnt++;  // otherwise increment use count

    return objects[object_id]->environment;
  }

  /** CACHE MISS -- Get it.
   */
  MDB_val key{sizeof(OID), &object_id};
  MDB_val value;

  MDB_txn *txn;
  if (mdb_txn_begin(db_env_[ENV_DB], nullptr, MDB_RDONLY, &txn) != 0) {
    throw internal_error("unable to open transaction to retrieve environment from db");
  }
  if (mdb_get(txn, db_dbi_[ENV_DB], &key, &value) != 0) {
    mdb_txn_abort(txn);
    throw internal_error("unable to retrieve environment from store");
  }
  if (mdb_txn_commit(txn) != 0) {
    throw internal_error("unable to end to commit read transaction to retrieve environment from db");
  }

  mica_string buffer((char *)value.mv_data, value.mv_size);

  Unserializer unserializer(buffer);

  // DO NOT REFCOUNT OBJECTS WHILE PAGING IN THE NEW ENVIRONMENT
  notify_start_paging();

  OStorage *result = unserializer.parseOStorage();

  objects[object_id]->environment = result;

  resolve(object_id);

  notify_end_paging();
  // START REFCOUNTING OBJECTS AGAIN

  /** Push entry to cache
   */
  push_cache(object_id);

  return objects[object_id]->environment;
}

void PersistentWorkspace::write(OID id) {
  /** If it's not cached, then it's not worth writing.
   */
  if (!objects[id]->environment)
    return;

  /** Must enforce cache put policy here
   */
  MDB_val key{sizeof(OID), &id};

  serialize_buffer serialized_form;
  objects[id]->environment->serialize_to(serialized_form);

  MDB_val value{serialized_form.size(), (void *)serialized_form.c_str()};

  MDB_txn *txn;
  if (mdb_txn_begin(db_env_[ENV_DB], nullptr, 0, &txn) != 0) {
    throw internal_error("unable to open transaction to store object");
  }
  if (mdb_put(txn, db_dbi_[ENV_DB], &key, &value, 0) != 0) {
    mdb_txn_abort(txn);
    throw internal_error("unable to write object to db");
  }
  if (mdb_txn_commit(txn) != 0) {
    throw internal_error("unable to end to commit write transaction to write object to db");
  }
}

Object *PersistentWorkspace::new_object() {
  Object *result = this->Workspace::new_object();

  push_cache(result->oid_);

  return result;
}

void PersistentWorkspace::write_object(OID id) {
  MDB_val key{sizeof(OID), &id};


  /** Just the refcnt
   */
  reference_counted::refcount_type refcnt = objects[id]->object->refcnt;
  MDB_val value{sizeof(reference_counted::refcount_type), &refcnt};

  MDB_txn *txn;
  if (mdb_txn_begin(db_env_[OID_DB], nullptr, 0, &txn) != 0) {
    throw internal_error("unable to open transaction to store refcnt");
  }
  if (mdb_put(txn, db_dbi_[OID_DB], &key, &value, 0) != 0) {
    mdb_txn_abort(txn);
    throw internal_error("unable to write refcnt to db");
  }
  if (mdb_txn_commit(txn) != 0) {
    throw internal_error("unable to end to commit write transaction to write refcnt to db");
  }
}

Ref<Object> PersistentWorkspace::resolve(OID id) {
  if (objects.size() <= id) {
    objects.resize((id + 1) * 2);  // add 1 so special case of 0
    // is not a problem
  }

  /** Check to see if there's already an object there
   */
  if (objects[id] && objects[id]->object) {
  } else {
    /** We don't even have an object entry for it
     */
    if (!objects[id]) {
      /** Empty object entry - we'll fill it in later.
       */
      objects[id] = new ObjectEntry(0, 0);
    }

    /** Now create the object
     */
    objects[id]->object = new Object(wid_, id);

    /** Retrieve object's reference count
     */
    MDB_val key{sizeof(OID), &id};
    MDB_val value;
    MDB_txn *txn;
    if (mdb_txn_begin(db_env_[OID_DB], nullptr, MDB_RDONLY, &txn) != 0) {
      throw internal_error("unable to open transaction to retrieve refcount from db");
    }
    int get_result = mdb_get(txn, db_dbi_[OID_DB], &key, &value);
    if (get_result == MDB_NOTFOUND) {
      // warn!
    } else if (get_result != 0) {
      mdb_txn_abort(txn);
      throw internal_error("unable to retrieve refcount from store");
    }
    if (mdb_txn_commit(txn) != 0) {
      throw internal_error("unable to end to commit read transaction to refcount environment from db");
    }

    // Handle miss in mdb_get!

    reference_counted::refcount_type refcnt;
    memcpy(&refcnt, value.mv_data, sizeof(reference_counted::refcount_type));
    objects[id]->object->refcnt = refcnt;
  }

  return objects[id]->object;
}

bool PersistentWorkspace::exists(OID id) {
  bool found = false;

  MDB_val key{sizeof(OID), &id};
  MDB_val value;
  MDB_txn *txn;
  if (mdb_txn_begin(db_env_[OID_DB], nullptr, MDB_RDONLY, &txn) != 0) {
    throw internal_error("unable to open transaction to retrieve refcount from db");
  }
  int get_result = mdb_get(txn, db_dbi_[OID_DB], &key, &value);
  if (get_result == MDB_NOTFOUND) {
    found = false;
  } else if (get_result != 0) {
    mdb_txn_abort(txn);
    throw internal_error("unable to retrieve refcount from store");
  } else {
    found = true;
  }
  if (mdb_txn_commit(txn) != 0) {
    throw internal_error("unable to end to commit read transaction to refcount environment from db");
  }
  return found;
}

}  // namespace mica

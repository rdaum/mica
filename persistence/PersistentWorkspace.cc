/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"

#include <sys/stat.h>

#include <iostream>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>

#include <boost/cast.hpp>

#include "Data.hh"
#include "Var.hh"
#include "Exceptions.hh"
#include "Unserializer.hh"
#include "List.hh"
#include "GlobalSymbols.hh"

#include "Object.hh"

#include "Pool.hh"
#include "Pools.hh"
#include "Symbol.hh"

#include "PersistentPool.hh"

#include "logging.hh"

using namespace mica;
using namespace std;

#define CACHE_WIDTH 64
#define CACHE_GROW_WINDOW 32

pair<PID, Var> PersistentPool::open( const Symbol &name, 
				     const Ref<Object> &parent_lobby )
{
  PersistentPool *pool = new (aligned) PersistentPool( name );
  pool->pid = Pools::instance.add( name, pool );

  pool->initialize();
  
  /** Lobby is always the first object in the pool.  Try to get it.
   *  If this fails, create it.
   */
  if (!pool->exists(0)) {
    pool->lobby = Object::create( pool->pid, parent_lobby )->asRef<Object>();

  } else {

    /** If there's a lobby, there's a task db, too.
     */
    pool->load_tasks();

    pool->lobby = pool->resolve(0);

  }


  return make_pair( pool->pid, pool->lobby );
}

PersistentPool::PersistentPool( const Symbol &poolName )
  : Pool( poolName ),
    cache_width(CACHE_WIDTH),
    cache_grow_window(CACHE_GROW_WINDOW)
{
  for (int i = 0 ; i < NUM_DBS ; i++ ) {
    databases[i] = new (aligned) Db( NULL, DB_CXX_NO_EXCEPTIONS );

    int ret;
    if ((ret = databases[i]->set_cachesize( 0, 10 * 1024 * 1024, 1)) != 0) {
      throw internal_error("unable to set DB cache size");
    }
    
    if ((ret = databases[i]->set_pagesize( 4096 )) != 0) {
      throw internal_error("unable to set DB page size");
    }
    databases[i]->set_errfile( stderr );
  }
}

void PersistentPool::initialize()
{
  /** Set name for each DB
   */
  mica_string basename = poolName.tostring();
  for (int i = 0 ; i < NUM_DBS; i++)
    names[i] = basename;

  names[ENV_DB].append(".env");
  names[OID_DB].append(".oid");

  /** Open each DB file
   */

  for (int i = 0; i < NUM_DBS; i++) {

    /* Open berkeley db file.  Create if necessary.
     */
    int flags = DB_CREATE;

    int ret;
    ret = databases[i]->open(
			     /** Version 4.1 of Berkeley DB wants one more arg
			      *  here.
			      */
#if (DB_VERSION_MAJOR == 4 && DB_VERSION_MINOR == 1)
			     NULL,
#endif
			     names[i].c_str(), NULL,
			     DB_HASH, flags, S_IRWXU );
    
    if (ret != 0) {
      throw internal_error("cannot open db");
    }
  }


}

void PersistentPool::sync()
{
  this->Pool::sync();

  // First we do a cache cleanup.

  // First pass, 
  for (vector<ObjectEntry*>::iterator oe = objects.begin();
       oe != objects.end(); oe++) {

    
  }

  // Write all object handles and environments to disk
  for (vector<ObjectEntry*>::iterator oe = objects.begin();
       oe != objects.end(); oe++) {
    if (*oe) {
      OID oid = oe - objects.begin();

      ObjectEntry *entry = *oe;
      if (entry->object)
	write_object(oid);
      if (entry->environment)
 	write(oid);
    }
  }

  for (int i = 0 ; i < NUM_DBS; i++)
    databases[i]->sync( 0 );
}

void PersistentPool::close()
{

  /** Save the task list for the next open session
   */
  save_tasks();

  sync();



  for (int i = 0 ; i < NUM_DBS; i++) {
    databases[i]->close( 0 );
    delete databases[i];
  }

  this->Pool::close();


}


void PersistentPool::del( OID idx )
{
  Dbt key;
  key.set_data( &idx );
  
  int ret;
  if ((ret = databases[OID_DB]->del( NULL, &key, 0 ))) {
    throw internal_error("unable to remove object id from db");
  }
  if ((ret = databases[ENV_DB]->del( NULL, &key, 0 ))) {
    throw internal_error("unable to remove environment from db");
  }

  /** Turf the cache entry for this object -- just reduce the use count
   *  to 0 and set the "deleted" flag.  It will be removed at the
   *  next cache flush.
   */
  int cache_id;
  if ((cache_id = objects[idx]->cache_id) != -1)
    cache_list[cache_id].deleted = true;
      
  this->Pool::del(idx);
}

inline void file_error() {
  char errmsg[80];
  snprintf( errmsg, 80, "error reading task file: %s", strerror(errno) );
  throw internal_error( errmsg );
}

void PersistentPool::load_tasks() {

  notify_start_paging(  );

  /** Open the task file - read only.
   */
  mica_string task_fname( poolName.tostring() );
  task_fname.append( ".tsk" );
  int flags = O_RDONLY;
  int task_fd = ::open( task_fname.c_str(), flags, S_IRWXU );

  /** Now iterate through it and retrieve each task
   */
  while(1) {
    size_t serialized_size;
    int error = read( task_fd, &serialized_size, sizeof(serialized_size) );
    if (error == 0)
      break;
    else if (error == -1) {
      file_error();
    }
    
    char buffer[serialized_size];
    error = read( task_fd, buffer, serialized_size );
    if (error != boost::numeric_cast<int>(serialized_size)) {
      throw internal_error("unable to read task from task list");
    } else if (error == -1) {
      file_error();
    }
    
    mica_string buffer_string( buffer, serialized_size );
    Unserializer unserializer(buffer_string);
    
    Ref<Task> task( unserializer.parseTaskReal() );

    logger.debugStream() << "pool " << pid << " retrieved task " << (Task*)task << " tid: " << task->tid << " msgid: " << task->msg_id << " refcnt: " << task->refcnt << log4cpp::CategoryStream::ENDLINE;

    if (task->tid >= managed_tasks.size());
      managed_tasks.resize( task->tid + 64 );
    
    managed_tasks[task->tid] = new (aligned) TaskEntry( (Task*)task, task->tid );
  } 
  ::close(task_fd);

  notify_end_paging();

  /** Now put all the blank spots in the task list into the free
   *  list.
   */
  for (vector<TaskEntry*>::reverse_iterator f = managed_tasks.rbegin(); 
       f != managed_tasks.rend(); f++) {
    if (!(*f)) {
      free_task_list.push_back( (managed_tasks.rend() - f) - 1 );
    }
  }
}

void PersistentPool::save_tasks() {

  reference_counted::collect_cycles();

  notify_start_paging( );

  /** Open the task file - write, truncate.
   */
  mica_string task_fname( poolName.tostring() );
  task_fname.append( ".tsk" );
  int flags = O_WRONLY | O_CREAT | O_TRUNC;
  int task_fd = ::open( task_fname.c_str(), flags, S_IRWXU );

  /** Now for each task in managed_tasks, write...
   */
  for (vector<TaskEntry*>::iterator ti = managed_tasks.begin(); 
       ti != managed_tasks.end(); ti++) {
    if (*ti) {
      Task *task = (*ti)->task;
      
      logger.debugStream() << "pool " << pid << " serializing: " << task << " tid: " << task->tid << " msgid: " << task->msg_id << " refcnt: " << task->refcnt << log4cpp::CategoryStream::ENDLINE;

      mica_string buffer( task->serialize_full() );

      /** Write size first.
       */
      size_t buffer_size = buffer.size();
    
      ::write( task_fd, &buffer_size, sizeof(buffer_size) );

      /** Now write the string
       */
      ::write( task_fd, buffer.c_str(), buffer_size );
    }
  }

  ::close( task_fd );

  notify_end_paging();
}

void PersistentPool::push_cache( OID oid ) {

  /** Pushes a new object to the cache.  Before doing so, we free up
   *  anything whose usecnt isn't up to spec.
   *  We don't flush unless the cache is full to the point of
   *  being bigger than the max cache_width PLUS the grow window
   *  We never flush the cache while collecting cycles.
   */
  if (!cycle_collecting() &&
      cache_list.size() > (cache_width + cache_grow_window))
    flush_cache();

  /** Now push the object onto the end of the cache
   */
  unsigned int cache_id = cache_list.size();
  cache_list.push_back( CacheEntry( oid, 1 ) );
  objects[oid]->cache_id = cache_id;
}

void PersistentPool::flush_cache() {

  // Sort the cache_list in descending order.  This puts objects with
  // the lowest use count at the end
  // O(N Log N) on average.  Maybe better to try and use nth_element (O(N))
  sort( cache_list.begin(), cache_list.end() );
  
  // Count cache_width elements in -- anything after that is expired
  // and everything before it is decremented by the usecnt of that element
  CacheVector::iterator last_valid = cache_list.begin() + cache_width;
  unsigned int lowest_use_cnt = last_valid->usecnt;

  // Now we go through and expire everything after it
  for (CacheVector::iterator expire = last_valid +1 ;
       expire != cache_list.end(); expire++) {

    if (!expire->deleted && objects[expire->object_id]
	&& objects[expire->object_id]->object) {

      write_object( expire->object_id );     

      // Sync and then turf the environment.  But only if there is one.      
      if (objects[expire->object_id]->environment) {
	// PAGING MODE ON (paging environment out of cache)
	notify_start_paging();
	write( expire->object_id );
	delete objects[expire->object_id]->environment;
	
	objects[expire->object_id]->environment = 0;
	objects[expire->object_id]->cache_id = -1;
	notify_end_paging();
	// PAGING MODE OFF 

      }
    }
  }
  
  // Clear out the entries just expired:
  cache_list.resize( cache_width );

  // Now go through and do two things:
  //    subtract the lowest_use_cnt from each use count
  //    reset the object's cache entry id to its new location
  for (CacheVector::iterator renum = cache_list.begin(); 
       renum != cache_list.end(); renum++) {
    renum->usecnt -= lowest_use_cnt;

    int c_id = renum - cache_list.begin();
    OID oid = renum->object_id;
    if (!renum->deleted && objects[oid])
      objects[oid]->cache_id = c_id;
  }

  assert( objects[cache_list[0].object_id]->cache_id == 0);

}

Environment *PersistentPool::get_environment( OID object_id ) {

  /** CACHE HIT
   */
  if (objects[object_id]->environment) {

    int cache_id = objects[object_id]->cache_id;

    /** If it has no cache entry, then push it to cache!
     */
    if (cache_id == -1)
      push_cache( object_id );
    else
      cache_list[cache_id].usecnt++;  // otherwise increment use count 

    return objects[object_id]->environment;
  }

  /** CACHE MISS -- Get it.
   */
  Dbt key;
  key.set_data( &object_id );
  key.set_size( sizeof(OID) );
  key.set_ulen( sizeof(OID) );

  Dbt value;

  int ret;
  if ((ret = databases[ENV_DB]->get( NULL, &key, &value, 0)) != 0) 
    throw internal_error("unable to retrieve environment from store");

  mica_string buffer( (char*)value.get_data(), value.get_size() );

  Unserializer unserializer(buffer);

  // DO NOT REFCOUNT OBJECTS WHILE PAGING IN THE NEW ENVIRONMENT
  notify_start_paging();

  Environment *result = unserializer.parseEnvironment();


  objects[object_id]->environment = result;

  resolve(object_id);

  notify_end_paging();
  // START REFCOUNTING OBJECTS AGAIN

  /** Push entry to cache
   */
  push_cache( object_id );

  return objects[object_id]->environment;
}

void PersistentPool::write( OID id )
{
  /** If it's not cached, then it's not worth writing.
   */
  if (!objects[id]->environment)
    return;

  /** Must enforce cache put policy here
   */
  Dbt key;
  key.set_data( &id );
  key.set_size( sizeof(OID) );
  key.set_ulen( sizeof(OID) );

  mica_string serialized_form = objects[id]->environment->serialize();

  Dbt value;
  value.set_data( (void*)serialized_form.c_str() );
  value.set_size( serialized_form.size() );
  value.set_ulen( serialized_form.size() );

  int ret;
  if ((ret = databases[ENV_DB]->put( NULL, &key, &value, 0)) != 0) 
    throw internal_error("unable to store environment in db");
}

Object *PersistentPool::new_object() {
  Object *result = this->Pool::new_object();

  push_cache( result->oid );

  return result;
}

void PersistentPool::write_object( OID id )
{
  Dbt key;
  key.set_data( &id );
  key.set_size( sizeof(OID) );
  key.set_ulen( sizeof(OID) );

  /** Just the refcnt
   */
  Dbt value;
  int refcnt = objects[id]->object->refcnt;
  value.set_data( &refcnt );
  value.set_size( sizeof(int) );
  value.set_ulen( sizeof(int) );

  int ret;
  if ((ret = databases[OID_DB]->put( NULL, &key, &value, 0)) != 0) 
    throw internal_error("unable to store object refcount in db");
}

Ref<Object> PersistentPool::resolve( OID id )
{

  if (objects.size() <= id) {
    objects.resize( (id+1) * 2 );  // add 1 so special case of 0
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
      objects[id] = new (aligned) ObjectEntry( 0, 0 );
    }

    /** Now create the object
     */
    objects[id]->object = new (aligned) Object( pid, id );
    
    /** Retrieve object's reference count
     */
    Dbt key;
    key.set_data( &id );
    key.set_size( sizeof(OID) );
    key.set_ulen( sizeof(OID) );
    
    Dbt value;
    
    int ret;
    if ((ret = databases[OID_DB]->get( NULL, &key, &value, 0)) != 0) 
      throw internal_error("unable to retrieve object refcount from db");
    
    int refcnt;
    memcpy( &refcnt, value.get_data(), sizeof(int) );
    objects[id]->object->refcnt = refcnt;
  } 

  return objects[id]->object;
}

bool PersistentPool::exists( OID id )
{
  /** Attempt to find object.
   */
  Dbt key;
  key.set_data( &id );
  key.set_size( sizeof(OID) );
  key.set_ulen( sizeof(OID) );

  Dbt value;
   
  int ret;
  if ((ret = databases[OID_DB]->get( NULL, &key, &value, 0)) != 0) 
    return false;
  else
    return true;
}

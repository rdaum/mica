/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef TYPES_WORKSPACE_HH
#define TYPES_WORKSPACE_HH

#include <boost/tuple/tuple.hpp>
#include <vector>

#include "base/Ref.hh"

#include "types/hash.hh"
#include "types/Object.hh"
#include "types/Symbol.hh"

namespace mica {

typedef unsigned int WID;
typedef unsigned int OID;
typedef unsigned int TID;

class OStorage;
class Object;
class Task;

/** A Workspace is a way of mapping object oids to actual physical environments,
 *  and maintaining a list of such handles for use in persistence or remote
 *  access.
 *
 *  @see Object
 *  @see PersistentPool
 */
class Workspace {
 public:
  /** Open a new pool of this type.
   *  Meant to be used instead of the constructor
   *  @param name the name (a Symbol) that identifies this pool
   *  @param parent_lobby a parent lobby that the lobby for this pool
   *                      should inherit from (or NONE)
   *  @return returns a tuple of: < pool id of the pool,
   *                                lobby (namespace) object for the pool >
   */
  static boost::tuple<WID, Var> open(const Symbol &name,
                                     const Ref<Object> &parent_lobby = Ref<Object>(0));

  /** Blank virtual destructor here to satisfy the compiler
   */
  virtual ~Workspace(){};

 public:
  // Workspace-related services.

  /** @return the PID of this pool.
   */
  WID getPid() const { return wid_; };

  /** Sync this pool (to disk, etc.)
   */
  virtual void sync();

  /** Close this pool and remove it from the list of available pools
   *  Does not free the actual pool object.t
   */
  virtual void close();

 public:
  // Services for objects in the pool.

  /** @return a pointer (not reference counted) to a new object in
   *          this pool.
   */
  virtual Object *new_object();

  /** Retrieve an environment for an object.
   *  @param object_id the OID of the object in question
   *  @return a pointer to the environment for the object requested
   */
  virtual OStorage *get_environment(OID object_id);

  /** Write updates to an environment
   *  @param object_id the oid of the object to update
   */
  virtual void write(OID object_id);

  /** Remove an object from this pool.
   *  @param object_id the oid of the object to remove from the pool
   */
  virtual void eject(OID object_id);

  /** Retrieve a reference to an object itself.
   *  @param object_id the object id of the object requested
   *  @return a reference counted pointer to the desired object.
   */
  virtual Ref<Object> resolve(OID object_id);

 protected:
  virtual void del(OID object_id);

 public:
  typedef std::vector<unsigned int> FreeList;

  struct ObjectEntry {
    Object *object;
    OStorage *environment;

    int cache_id; /** Location in the cache vector -- used
                   *  by persistent pool only
                   */

    // A constructor for ease of use
    ObjectEntry(Object *entry_for, OStorage *the_environment)
        : object(entry_for), environment(the_environment), cache_id(-1) {}
  };

  typedef std::vector<ObjectEntry *> ObjectList;
  ObjectList objects;
  FreeList free_object_list;

 public:
  // ***** Services for tasks in the pool ******

  /** Retrieve a task managed in this pool
   *  @param the identifier for the task
   *  @return a pointer to a task object from the pool
   */
  Task *retrieve_task(TID task_id) const;

  /** Begin managing a task in this pool.  Sets the task id
   *  on the task object and stores a pointer to it internally.
   *  After this method is invoked, retrieve_task etc. can be called
   *  using the new ID.
   *  @param task pointer to a task object to begin managing
   *  @return TID the task id that was set on the task
   */
  TID manage_task(Task *task);

  /** Unmanage a task managed by this pool.  This is only really called
   *  when a task is garbage collected.
   *  @param tid the task id of the task
   */
  void unmanage_task(TID task_id);

 protected:
  struct TaskEntry {
    Task *task;
    TID tid;
    TaskEntry(Task *in_task, TID in_task_id) : task(in_task), tid(in_task_id) {}
  };
  typedef std::vector<TaskEntry *> TaskList;
  TaskList managed_tasks_;
  FreeList free_task_list_;

 public:
  WID wid_;
  Symbol pool_name_;
  Ref<Object> lobby_;

 protected:
  /** Constructor is protected.
   */
  Workspace(const Symbol &name);
};
};

#endif  // TYPES_WORKSPACE_HH

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Workspace.hh"

#include <cassert>
#include <cstdio>
#include <functional>


#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/List.hh"
#include "types/Object.hh"
#include "types/OStorage.hh"
#include "types/String.hh"
#include "types/Symbol.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "types/Workspaces.hh"

using namespace std;
using namespace mica;

boost::tuple<PID, Var> Pool::open(const Symbol &name, const Ref<Object> &parent_lobby) {
  Pool *pool = new (aligned) Pool(name);
  pool->pid = Pools::instance.add(name, pool);

  Var lobby_v;
  if ((Object *)parent_lobby)
    lobby_v = Object::create(pool->pid, parent_lobby);
  else
    lobby_v = Object::create(pool->pid);

  pool->lobby = lobby_v->asRef<Object>();

  return boost::tuple<PID, Var>(pool->pid, lobby_v);
}

Pool::Pool(const Symbol &name) : poolName(name), lobby(Ref<Object>(0)) {
  free_object_list.clear();
  free_task_list.clear();
}

void Pool::sync() {}

template <class Container, class FreeList>
unsigned int new_in(Container &container, FreeList &free_list) {
  unsigned int id;
  if (!free_list.empty()) {
    id = free_list.back();
    free_list.pop_back();
  } else {
    id = container.size();
    container.push_back(0);
  }
  return id;
}

/** OBJECT SERVICES
 */
Object *Pool::new_object() {
  /** Look in the free list for an available object id.
   */
  unsigned int id = new_in(objects, free_object_list);

  ObjectEntry *new_entry =
      new (aligned) ObjectEntry(new (aligned) Object(pid, id), new (aligned) OStorage());
  objects[id] = new_entry;

  write(id);

  return new_entry->object;
}

OStorage *Pool::get_environment(OID object_id) {
  assert(object_id < objects.size());
  assert(objects[object_id]);

  return objects[object_id]->environment;
}

/** Write updates of an environment
 */
void Pool::write(OID object_id) {
  assert(object_id < objects.size());
  assert(objects[object_id]);

  /** Does nothing in base implementation
   */
}

/** Destroys an object
 */
void Pool::eject(OID object_id) {
  assert(object_id < objects.size());
  assert(objects[object_id]);

  if (objects[object_id]->environment)
    delete objects[object_id]->environment;

  delete objects[object_id];
  objects[object_id] = 0;
  free_object_list.push_back(object_id);
}

Ref<Object> Pool::resolve(OID object_id) { return objects[object_id]->object->asRef<Object>(); }

void Pool::del(OID idx) {
  /** STUB
   */
}

void Pool::close() {
  for (ObjectList::iterator x = objects.begin(); x != objects.end(); x++) {
    ObjectEntry *S = *x;
    if (S) {
      notify_start_paging();

      if (S->environment) {
        delete S->environment;
      }

      if (S->object) {
        delete S->object;
      }

      delete S;
      notify_end_paging();
    }
  }
  objects.clear();

  Pools::instance.removePool(pid);
}

/*** TASK SERVICES
 **/
TID Pool::manage_task(Task *task) {
  /** Look in the free list for an available task id.
   */
  unsigned int id = new_in(managed_tasks, free_task_list);

  TaskEntry *new_entry = new TaskEntry(task, id);
  managed_tasks[id] = new_entry;

  return id;
}

Task *Pool::retrieve_task(TID task_id) const { return managed_tasks[task_id]->task; }

void Pool::unmanage_task(TID task_id) {
  delete managed_tasks[task_id];

  managed_tasks[task_id] = 0;
  free_task_list.push_back(task_id);
}

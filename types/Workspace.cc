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

boost::tuple<WID, Var> Workspace::open(const Symbol &name, const Ref<Object> &parent_lobby) {
  Workspace *pool = new (aligned) Workspace(name);
  pool->wid_ = Workspaces::instance.add(name, pool);

  Var lobby_v;
  if ((Object *)parent_lobby)
    lobby_v = Object::create(pool->wid_, parent_lobby);
  else
    lobby_v = Object::create(pool->wid_);

  pool->lobby_ = lobby_v->asRef<Object>();

  return boost::tuple<WID, Var>(pool->wid_, lobby_v);
}

Workspace::Workspace(const Symbol &name) : pool_name_(name), lobby_(Ref<Object>(0)) {
  free_object_list.clear();
  free_task_list_.clear();
}

void Workspace::sync() {}

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
Object *Workspace::new_object() {
  /** Look in the free list for an available object id.
   */
  unsigned int id = new_in(objects, free_object_list);

  ObjectEntry *new_entry =
      new (aligned) ObjectEntry(new (aligned) Object(wid_, id), new (aligned) OStorage());
  objects[id] = new_entry;

  write(id);

  return new_entry->object;
}

OStorage *Workspace::get_environment(OID object_id) {
  assert(object_id < objects.size());
  assert(objects[object_id]);

  return objects[object_id]->environment;
}

/** Write updates of an environment
 */
void Workspace::write(OID object_id) {
  assert(object_id < objects.size());
  assert(objects[object_id]);

  /** Does nothing in base implementation
   */
}

/** Destroys an object
 */
void Workspace::eject(OID object_id) {
  assert(object_id < objects.size());
  assert(objects[object_id]);

  if (objects[object_id]->environment)
    delete objects[object_id]->environment;

  delete objects[object_id];
  objects[object_id] = 0;
  free_object_list.push_back(object_id);
}

Ref<Object> Workspace::resolve(OID object_id) { return objects[object_id]->object->asRef<Object>(); }

void Workspace::del(OID idx) {
  /** STUB
   */
}

void Workspace::close() {
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

  Workspaces::instance.removePool(wid_);
}

/*** TASK SERVICES
 **/
TID Workspace::manage_task(Task *task) {
  /** Look in the free list for an available task id.
   */
  unsigned int id = new_in(managed_tasks_, free_task_list_);

  TaskEntry *new_entry = new TaskEntry(task, id);
  managed_tasks_[id] = new_entry;

  return id;
}

Task *Workspace::retrieve_task(TID task_id) const { return managed_tasks_[task_id]->task; }

void Workspace::unmanage_task(TID task_id) {
  delete managed_tasks_[task_id];

  managed_tasks_[task_id] = 0;
  free_task_list_.push_back(task_id);
}

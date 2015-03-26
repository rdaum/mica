/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Workspaces.hh"

#include <cassert>
#include <cstdio>


#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "types/Object.hh"
#include "types/OStorage.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "types/Workspace.hh"

using namespace mica;
using namespace std;

/** Static global singleton
 */
Workspaces Workspaces::instance;

Workspaces::Workspaces() {
  workspaces_.clear();
  default_workspace_ = 0;
}

Workspaces::~Workspaces() {
  NamesMap::iterator ni;
  for (ni = names_.begin(); ni != names_.end(); ni++) {
    Workspace *x = workspaces_[ni->second];
    delete x;
  }

  names_.clear();
  workspaces_.clear();
}

std::vector<Workspace *> Workspaces::pools() const { return workspaces_; }

Workspace *Workspaces::get(WID pool) const {
  Workspace *poolO;
  if (pool >= workspaces_.size() || !(poolO = workspaces_[pool])) {
    char errstr[50];
    snprintf(errstr, 50, "pool %d not found", pool);
    throw internal_error(errstr);
  }
  return poolO;
}

void Workspaces::removePool(WID pool) {
  if (pool >= workspaces_.size())
    throw internal_error("pool not found");

  workspaces_[pool] = (Workspace *)0;
  NamesMap::iterator ni;
  for (ni = names_.begin(); ni != names_.end(); ni++)
    if (ni->second == pool)
      break;

  if (ni != names_.end())
    names_.erase(ni);
  else
    throw internal_error("name mapping for pool not found");
}

WID Workspaces::add(const Symbol &name, Workspace *pool) {
  WID idx = workspaces_.size();
  workspaces_.push_back(pool);
  names_[name] = idx;

  return idx;
}

void Workspaces::setDefault(WID pool) { default_workspace_ = pool; }

WID Workspaces::getDefault() const { return default_workspace_; }

Workspace *Workspaces::find_pool_by_name(const Symbol &poolName) const {
  NamesMap::const_iterator ni;
  ni = names_.find(poolName);

  if (ni == names_.end())
    throw not_found("object pool not found");

  return get(ni->second);
}

void Workspaces::remove(const Var &obj) {
  if (obj.type_identifier() != Type::OBJECT)
    throw invalid_type("unable to remove non-object from pool");

  Ref<Object> handle = obj->asRef<Object>();

  return get(handle->wid_)->eject(handle->oid_);
}

void Workspaces::close() {
  /** Don't close the first one
   */
  vector<Workspace *>::iterator pi;
  for (pi = workspaces_.begin() + 1; pi != workspaces_.end(); pi++) {
    (*pi)->close();
    delete (*pi);
  }
}

void Workspaces::sync() {
  vector<Workspace *>::iterator pi;
  for (pi = workspaces_.begin(); pi != workspaces_.end(); pi++) {
    (*pi)->sync();
  }
}

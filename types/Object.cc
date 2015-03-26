/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Object.hh"

#include <boost/tuple/tuple.hpp>
#include <cassert>
#include <glog/logging.h>
#include <iostream>
#include <sstream>

#include "base/Ref.hh"
#include "base/Ref.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/List.hh"
#include "types/OStorage.hh"
#include "types/Symbol.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "types/Workspace.hh"
#include "types/Workspaces.hh"


using namespace mica;
using namespace std;

void Object::finalize_paged_object() {
  LOG(INFO) << "Collecting unused Object pid: " << wid_ << " oid: " << oid_;
  Workspace *pool = Workspaces::instance.get(wid_);
  pool->eject(oid_);
}

Object::Object(OID wid, WID oid) : wid_(wid), oid_(oid) {
  paged = true;
}

Var Object::create(int pool_id, const Ref<Object> &parent) {
  WID pid;
  if (pool_id == -1)
    pid = Workspaces::instance.getDefault();
  else
    pid = pool_id;

  Workspace *pool = Workspaces::instance.get(pid);
  Object *self = pool->new_object();

  if ((Object *)parent) {
    Var parent_v(parent);
    self->environment()->add_delegate(self, PARENT_SYM, parent_v);
  }

  /** This young object is dirty! (spank spank)
   */
  self->write();

  return Var(self);
}

Var Object::clone() const { return Object::create(wid_, this->asRef<Object>()); }

var_vector Object::delegates() const { return environment()->delegates(); }

OStorage *Object::environment() const { return Workspaces::instance.get(wid_)->get_environment(oid_); }

/** Called after all mutations.
 */
void Object::write() { Workspaces::instance.get(wid_)->write(oid_); }

OptSlot Object::get(const Var &accessor, const Symbol &name) const {
  /** Attempt to actually get the value.
   */
  OptVar result(environment()->getLocal(accessor, name));
  if (result)
    return OptSlot(Slot(this, *result));
  else
    return OptSlot();
}

Var Object::declare(const Var &accessor, const Symbol &name, const Var &value) {
  if (environment()->addLocal(accessor, name, value))
    /** Write it to the pool
     */
    write();
  else
    throw E_SLOTOVLD;

  return value;
}

Var Object::assign(const Var &accessor, const Symbol &name, const Var &value) {
  if (environment()->replaceLocal(accessor, name, value))
    /** I'm dirty now
     */
    write();
  else
    throw E_SLOTNF;

  return value;
}

void Object::remove(const Var &accessor, const Symbol &name) {
  if (environment()->removeLocal(accessor, name))
    /** I'm dirty now
     */
    write();
  else
    throw E_SLOTNF;
}

Var Object::slots() const { return environment()->slots(); }

bool Object::operator==(const Object &obj) const { return &obj == this; }

bool Object::operator==(const Var &rhs) const {
  if (rhs.type_identifier() != Type::OBJECT)
    return false;

  Object *x = (rhs->asType<Object *>());

  return this == x;
}

mica_string Object::rep() const {
  OptVar result(environment()->getLocal(Var(const_cast<Object *>(this)), TITLE_SYM));

  if (result) {
    mica_string dstr;
    dstr.append("<object (.name: ");
    dstr.append(result->rep());
    dstr.append(") >");
    return dstr;
  } else
    return "<object>";
}

var_vector Object::perform(const Ref<Frame> &caller, const Var &args) {
  OptVar result(environment()->getLocal(Var(VERB_SYM), PERFORM_SYM));

  if (result)
    return result->perform(caller, args);
  else
    throw E_SLOTNF;
}

/** this should only serialize the object and not the environment --
 *  that should be left to the Workspace to do
 */
void Object::serialize_to(serialize_buffer &s_form) const {
  Pack(s_form, type_identifier());

  /** Serialize the handle information
   */
  s_form.append(Workspaces::instance.get(wid_)->pool_name_.serialize());

  Pack(s_form, oid_);
}

bool Object::operator<(const Var &rhs) const {
  if (rhs.type_identifier() != Type::OBJECT)
    return false;

  Object *x = (rhs->asType<Object *>());

  return this < x;
}

bool Object::truth() const { return true; }

Var Object::add(const Var &rhs) const { throw invalid_type("addition of Object"); }

Var Object::sub(const Var &rhs) const { throw invalid_type("subtraction of Object"); }

Var Object::mul(const Var &rhs) const { throw invalid_type("multiplication of Object"); }

Var Object::div(const Var &rhs) const { throw invalid_type("division of Object"); }

Var Object::mod(const Var &rhs) const { throw invalid_type("modulus of Object"); }

Var Object::neg() const { throw invalid_type("modulus of Object"); }

Var Object::inc() const { throw invalid_type("increment of Object"); }

Var Object::dec() const { throw invalid_type("decrement of Object"); }

unsigned int Object::length() const { throw invalid_type("objects have no length"); }

int Object::toint() const { throw invalid_type("cannot convert Object to integer"); }

float Object::tofloat() const { throw invalid_type("cannot convert Object to float"); }

bool Object::isNumeric() const { return false; }

mica_string Object::tostring() const { throw invalid_type("cannot convert Object to string"); }

bool Object::isObject() const { return true; }

void Object::append_child_pointers(child_set &child_list) {
  environment()->append_child_pointers(child_list);
}

void Object::set_verb_parasite(const Symbol &name, unsigned int pos,
                               const var_vector &argument_template, const Var &definer,
                               const Var &method) {
  environment()->set_verb_parasite(name, pos, argument_template, definer, method);

  write();
}

VerbList Object::get_verb_parasites(const Symbol &name, unsigned int pos) const {
  return environment()->get_verb_parasites(name, pos);
}

void Object::rm_verb_parasite(const Symbol &name, unsigned int pos,
                              const var_vector &argument_template) {
  environment()->rm_verb_parasite(name, pos, argument_template);
  write();
}

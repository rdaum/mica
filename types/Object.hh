/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef OBJECT_HH
#define OBJECT_HH


#include "types/ArgumentMask.hh"
#include "types/Atom.hh"
#include "types/OStorage.hh"
#include "types/Workspaces.hh"

namespace mica {

typedef unsigned int WID;
typedef unsigned int OID;

/** Holds an environment and provides inheritance.
 */
class Object : public Atom {
 public:
  Type::Identifier type_identifier() const { return Type::OBJECT; }

 public:
  /** Workspace and object id
   */
  WID wid_;
  OID oid_;
  ArgumentMask arg_mask_;

 protected:
  friend class Workspace;
  friend class Workspaces;
  friend class PersistentWorkspace;

  Object(WID wid, OID oid);

 public:
  static Var create(int pool_id = -1, const Ref<Object> &parent = Ref<Object>(0));

  Var clone() const;

  var_vector delegates() const;

 public:
  /** Declare a slot on an object
   *  @param the accessor of the slot, or None if public
   *  @param name the symbol to create
   *  @return the slot value
   */
  Var declare(const Var &accessor, const Symbol &name, const Var &value);

  /** Search for a slot by accessor and name
   *  @param accessor the accessor object used for use during the search
   *  @param name the symbol to search for
   *  @return copy of value
   *  @throws not_found
   */
  OptSlot get(const Var &accessor, const Symbol &name) const;

  /** assign a value to a slot
   *  @param accessor the accessor of the slot, or None if public
   *  @param name the symbol to set
   *  @param value the value to set the slot to
   *  @return copy of value
   *  @throws slot_not_found
   */
  Var assign(const Var &accessor, const Symbol &name, const Var &value);

  /** remove a slot
   *  @param accessor the accessor of the slot, or None if public
   *  @param name the symbol of the slot
   *  @throws slot_not_found
   */
  void remove(const Var &accessor, const Symbol &name);

  /** @return a list of slots implemented on this object
   */
  Var slots() const;

 public:
  void set_verb_parasite(const Symbol &name, unsigned int pos, const var_vector &argument_template,
                         const Var &definer, const Var &method);

  VerbList get_verb_parasites(const Symbol &name, unsigned int pos) const;

  void rm_verb_parasite(const Symbol &name, unsigned int pos, const var_vector &argument_template);

 public:
  /** Returns true -- yes, this is an object (prototype)
   */
  bool isObject() const;

 public:
  /** Forwards to :perform
   */
  var_vector perform(const Ref<Frame> &caller, const Var &args);

 public:
  mica_string rep() const;

  void serialize_to(serialize_buffer &s_form) const;

 public:
  bool operator==(const Object &obj) const;

  bool operator==(const Var &rhs) const;

  bool operator<(const Var &v2) const;

  bool truth() const;

  Var add(const Var &rhs) const;

  Var div(const Var &rhs) const;

  Var mul(const Var &rhs) const;

  Var sub(const Var &rhs) const;

  Var mod(const Var &rhs) const;

  Var neg() const;

  Var inc() const;

  Var dec() const;

  unsigned int length() const;

  int toint() const;

  float tofloat() const;

  bool isNumeric() const;

  mica_string tostring() const;

  void finalize_paged_object();

 public:
  void append_child_pointers(child_set &child_list);

 public:
  OStorage *environment() const;

  void write();
};
}

#endif /* OBJECT_HH */

#include "types/OStorage.hh"

#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/List.hh"
#include "types/Object.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"

using namespace std;
using namespace mica;

void VerbDef::append_child_pointers(child_set &child_list) {
  child_list << definer << method;

  append_datas(child_list, argument_template);
}

VerbDef::VerbDef() : definer(NONE), method(NONE) { argument_template.clear(); }

VerbDef::VerbDef(const VerbDef &x)
    : definer(x.definer), argument_template(x.argument_template), method(x.method) {}

bool VerbDef::operator==(const VerbDef &x) const {
  return definer == x.definer && argument_template == x.argument_template && method == x.method;
}

bool VerbDef::operator<(const VerbDef &x) const {
  return definer < x.definer || argument_template < x.argument_template || method < x.method;
}

VerbDef &VerbDef::operator=(const VerbDef &x) {
  if (&x == this)
    return *this;

  definer = x.definer;
  method = x.method;
  argument_template = x.argument_template;

  return *this;
}

unsigned int hash_verb_pair::operator()(const pair<Symbol, unsigned int> &p) const {
  return (p.first.idx << 16) + p.second;
}

OStorage::OStorage() { delegates_iterator = mOptSlots.end(); }

void OStorage::add_delegate(const Object *from, const Symbol &name, const Var &delegate) {
  addLocal(Var(DELEGATE_SYM), name, delegate);

  delegates_iterator = mOptSlots.find(Var(DELEGATE_SYM));
}

var_vector OStorage::delegates() {
  if (delegates_iterator == mOptSlots.end())
    delegates_iterator = mOptSlots.find(Var(DELEGATE_SYM));

  var_vector delegates_vec;
  if (delegates_iterator != mOptSlots.end()) {
    for (OptSlotList::const_iterator sl_i = delegates_iterator->second.begin();
         sl_i != delegates_iterator->second.end(); sl_i++) {
      delegates_vec.push_back(sl_i->second);
    }
  }

  return delegates_vec;
}

OStorage::~OStorage() { mOptSlots.clear(); }

OptVar OStorage::getLocal(const Var &accessor, const Symbol &name) const {
  // Find by accessor.
  OptSlotMap::const_iterator am_i = mOptSlots.find(accessor);

  // Scan the OptSlotList for an accessor match
  if (am_i != mOptSlots.end()) {
    // Found, look for the name
    OptSlotList::const_iterator sl_i = am_i->second.find(name);
    if (sl_i != am_i->second.end()) {
      return OptVar(sl_i->second);
    }
  }
  return OptVar();
}

bool OStorage::removeLocal(const Var &accessor, const Symbol &name) {
  // Find by accessor.
  OptSlotMap::iterator am_i = mOptSlots.find(accessor);

  if (am_i != mOptSlots.end()) {
    // Scan the OptSlotList for name match
    OptSlotList::iterator sl_i = am_i->second.find(name);
    if (sl_i != am_i->second.end()) {
      am_i->second.erase(sl_i);
      return true;
    }
  }
  return false;
}

Var OStorage::slots() const {
  var_vector slots;

  for (OptSlotMap::const_iterator am_i = mOptSlots.begin(); am_i != mOptSlots.end(); am_i++) {
    for (OptSlotList::const_iterator sl_i = am_i->second.begin(); sl_i != am_i->second.end();
         sl_i++) {
      var_vector slot_pair;
      slot_pair.push_back(am_i->first);
      slot_pair.push_back(sl_i->second);

      slots.push_back(List::from_vector(slot_pair));
    }
  }

  return List::from_vector(slots);
}

bool OStorage::addLocal(const Var &accessor, const Symbol &name, const Var &value) {
  // Find the accessor.
  OptSlotMap::iterator am_i = mOptSlots.find(accessor);

  // Found - look for name
  if (am_i != mOptSlots.end()) {
    OptSlotList::iterator sl_i = am_i->second.find(name);
    if (sl_i != am_i->second.end()) {
      // Already there, don't add.
      return false;
    }
  }

  mOptSlots[accessor][name] = value;

  return true;
}

bool OStorage::replaceLocal(const Var &accessor, const Symbol &name, const Var &value) {
  // Find the accessor.
  OptSlotMap::iterator am_i = mOptSlots.find(accessor);

  // Found - now find name
  if (am_i != mOptSlots.end()) {
    // Found, look for the name
    OptSlotList::iterator sl_i = am_i->second.find(name);
    if (sl_i != am_i->second.end()) {
      // Replace
      sl_i->second = value;
      return true;
    }
  }

  return false;
}

void OStorage::serialize_to(serialize_buffer &s_form) const {
  for (OptSlotMap::const_iterator am_i = mOptSlots.begin(); am_i != mOptSlots.end(); am_i++) {
    for (OptSlotList::const_iterator sl_i = am_i->second.begin(); sl_i != am_i->second.end();
         sl_i++) {
      Pack(s_form, true);
      s_form.append(sl_i->first.serialize());
      am_i->first.serialize_to(s_form);   // accessor
      sl_i->second.serialize_to(s_form);  // value
    }
  }
  Pack(s_form, false);

  /** Now store the verbs.  We do this a bit differently.
   *  We pack them all and then paste a special position arg at the end
   *  to indiciate the end of the list.  So we don't have to
   *  calculate the size before hand.
   */

  for (VerbParasiteMap::const_iterator am_i = verb_parasites.begin(); am_i != verb_parasites.end();
       am_i++) {
    /** First pack the position
     */
    Pack(s_form, am_i->first.second);

    /** Pack the name
     */
    s_form.append(am_i->first.first.serialize());

    /** Now pack the members of the verbdef
     */
    am_i->second->definer.serialize_to(s_form);
    SerializeVV(s_form, am_i->second->argument_template);
    am_i->second->method.serialize_to(s_form);
  }
  /** End of verbdef list marker (it's not possible to have
   *  an arg-pos this high, so this can suffice as a marker
   */
  Pack(s_form, END_OF_ARGS_MARKER);
}

void OStorage::append_child_pointers(child_set &children) {
  for (OptSlotMap::const_iterator am_i = mOptSlots.begin(); am_i != mOptSlots.end(); am_i++) {
    for (OptSlotList::const_iterator sl_i = am_i->second.begin(); sl_i != am_i->second.end();
         sl_i++) {
      children << am_i->first << sl_i->second;
    }
  }
  for (VerbParasiteMap::const_iterator am_i = verb_parasites.begin(); am_i != verb_parasites.end();
       am_i++) {
    children.push_back((VerbDef *)am_i->second);
  }
}

void OStorage::set_verb_parasite(const Symbol &name, unsigned int pos,
                                 const var_vector &argument_template, const Var &definer,
                                 const Var &method) {
  Ref<VerbDef> vd(new VerbDef());
  vd->definer = definer;
  vd->argument_template = argument_template;
  vd->method = method;

  pair<VerbParasiteMap::iterator, VerbParasiteMap::iterator> nm_i;
  nm_i = verb_parasites.equal_range(make_pair(name, pos));
  if (nm_i.first != verb_parasites.end()) {
    for (VerbParasiteMap::iterator tm_i = nm_i.first; tm_i != nm_i.second; tm_i++)
      if (tm_i->second->argument_template == argument_template) {
        tm_i->second = vd;
        return;
      }
  }
  verb_parasites.insert(make_pair(make_pair(name, pos), vd));
}

void OStorage::rm_verb_parasite(const Symbol &name, unsigned int pos,
                                const var_vector &argument_template) {
  pair<VerbParasiteMap::iterator, VerbParasiteMap::iterator> nm_i;
  nm_i = verb_parasites.equal_range(make_pair(name, pos));
  assert(nm_i.first != verb_parasites.end());

  for (VerbParasiteMap::iterator tm_i = nm_i.first; tm_i != nm_i.second; tm_i++)
    if (tm_i->second->argument_template == argument_template) {
      verb_parasites.erase(tm_i);
      return;
    }

  assert(0);
}

VerbList OStorage::get_verb_parasites(const Symbol &name, unsigned int pos) const {
  VerbList results;

  pair<VerbParasiteMap::const_iterator, VerbParasiteMap::const_iterator> nm_i;
  nm_i = verb_parasites.equal_range(make_pair(name, pos));

  if (nm_i.first == verb_parasites.end())
    return results;

  for (VerbParasiteMap::const_iterator tm_i = nm_i.first; tm_i != nm_i.second; tm_i++)
    results.push_back(tm_i->second);

  return results;
}

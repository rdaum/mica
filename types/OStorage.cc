#include "Data.hh"
#include "Var.hh"
#include "Object.hh"
#include "Environment.hh"
#include "Symbol.hh"
#include "List.hh"
#include "Exceptions.hh"
#include "GlobalSymbols.hh"

using namespace std;
using namespace mica;

child_set VerbDef::child_pointers() {
  child_set children(data_pair( definer, method ));
  append_datas( children, argument_template );
  return children;
}


VerbDef::VerbDef() 
  : definer(NONE), method(NONE)
{
  argument_template.clear();
}

VerbDef::VerbDef( const VerbDef &x )
  : definer(x.definer), argument_template(x.argument_template), method(x.method)
{
}

bool VerbDef::operator==( const VerbDef &x ) {
  return definer == x.definer && argument_template == x.argument_template &&
    method == x.method;
}

VerbDef &VerbDef::operator=( const VerbDef &x ) {
  if (&x == this)
    return *this;

  definer = x.definer;
  method = x.method;
  argument_template = x.argument_template;

  return *this;
}


Environment::Environment()
{
#ifndef JUDY
  mSlots.clear();
#endif
  delegates_iterator = mSlots.end();
}

void Environment::add_delegate( const Object *from, 
				const Symbol &name,
				const Var &delegate )
{
  addLocal( Var(DELEGATE_SYM), name, delegate );

  delegates_iterator = mSlots.find(Var(DELEGATE_SYM));
}

var_vector Environment::delegates() {
  if (delegates_iterator == mSlots.end())
    delegates_iterator = mSlots.find( Var(DELEGATE_SYM) );
  
  var_vector delegates_vec;
  if (delegates_iterator != mSlots.end()) {
    for (SlotList::const_iterator sl_i = delegates_iterator->second.begin();
	 sl_i != delegates_iterator->second.end(); sl_i++) {
      delegates_vec.push_back( sl_i->second->value );
    }
  }    

  return delegates_vec;
}

Environment::~Environment()
{
  for (SlotMap::const_iterator nm_i = mSlots.begin();
       nm_i != mSlots.end(); nm_i++) {
    for (SlotList::const_iterator sl_i = nm_i->second.begin();
	 sl_i != nm_i->second.end(); sl_i++) {
      delete sl_i->second;
    }
  }
  mSlots.clear();
}

SlotEntry *Environment::getLocal( const Var &accessor, 
				  const Symbol &name ) const 
{
  // Find by accessor.
  SlotMap::const_iterator am_i = mSlots.find( accessor );

  // Scan the SlotList for an accessor match
  if (am_i != mSlots.end()) {

    // Found, look for the name
    SlotList::const_iterator sl_i = am_i->second.find( name );
    if (sl_i != am_i->second.end()) {
      SlotEntry *slot_entry = sl_i->second;
      if (slot_entry->accessor == accessor)
	return slot_entry;
    }
  }
  return (SlotEntry*)0;
}


bool Environment::removeLocal( const Var &accessor, 
			       const Symbol &name ) {
  // Find by accessor.
  SlotMap::iterator am_i = mSlots.find( accessor );

  if (am_i != mSlots.end()) {
    // Scan the SlotList for name match
    SlotList::iterator sl_i = am_i->second.find( name );
    if (sl_i != am_i->second.end()) {
      delete sl_i->second;
      am_i->second.erase( sl_i );
      return true;
    }
  }
  return false;
}

Var Environment::slots() const
{

  var_vector slots;

  for (SlotMap::const_iterator am_i = mSlots.begin();
       am_i != mSlots.end(); am_i++) {
    for (SlotList::const_iterator sl_i = am_i->second.begin();
	 sl_i != am_i->second.end(); sl_i++) {
      SlotEntry *slot_entry = sl_i->second;
      var_vector slot_pair;
      slot_pair.push_back( slot_entry->accessor );
      slot_pair.push_back( Var(slot_entry->name) );

      slots.push_back( List::from_vector
(slot_pair) );
    }
  }

  return List::from_vector
(slots);

}

SlotEntry *Environment::addLocal( const Var &accessor, 
				  const Symbol &name, const Var &value )
{
  // Find the accessor.
  SlotMap::iterator am_i = mSlots.find( accessor );

  // Not found - begin the SlotLIst and return the slot
  if (am_i != mSlots.end()) {

    // Found, look for the name
    SlotList::iterator sl_i = am_i->second.find( name );
    if (sl_i != am_i->second.end())
      return sl_i->second;
    
  }

  // Not found, insert the slot
  SlotEntry *new_slot_entry = new (aligned) SlotEntry( name, accessor, value );
  mSlots[accessor][name] = new_slot_entry;

  return new_slot_entry;
}




rope_string Environment::serialize() const
{
  rope_string s_form;

  /** Append every single slot - flatten the entire hash->list
   *  to a vector and then write that
   */
  vector<SlotEntry*> slots;

  for (SlotMap::const_iterator am_i = mSlots.begin();
       am_i != mSlots.end(); am_i++) {
    for (SlotList::const_iterator sl_i = am_i->second.begin();
	 sl_i != am_i->second.end(); sl_i++) {
      slots.push_back( sl_i->second );
    }
  }
  
  Pack( s_form, slots.size() );
  for (vector<SlotEntry*>::iterator x = slots.begin();
       x != slots.end(); x++) {
    SlotEntry *slot_entry = *x;
    s_form.append( slot_entry->name.serialize() );
    s_form.append( slot_entry->accessor.serialize() );
    s_form.append( slot_entry->value.serialize() );
  }
  
  /** Now store the verbs.  We do this a bit differently.
   *  We pack them all and then paste a special position arg at the end
   *  to indiciate the end of the list.  So we don't have to
   *  calculate the size before hand.
   */

  for (VerbParasiteMap::const_iterator am_i = verb_parasites.begin();
       am_i != verb_parasites.end(); am_i++) {
      for (VerbTemplatesMap::const_iterator tl_i = am_i->second.begin();
	   tl_i != am_i->second.end(); tl_i++) {

	/** First pack the position
	 */
	Pack( s_form, am_i->first.second );

	/** Pack the name
	 */
	s_form.append( am_i->first.first.serialize() );

	/** Now pack the members of the verbdef
	 */
	s_form.append( tl_i->second->definer.serialize() );
	SerializeVV( s_form, tl_i->second->argument_template );
	s_form.append( tl_i->second->method.serialize() );
      }
  }
  /** End of verbdef list marker (it's not possible to have
   *  an arg-pos this high, so this can suffice as a marker
   */
  Pack( s_form, END_OF_ARGS_MARKER );

  return s_form;
}

child_set Environment::child_pointers() {
  child_set children;


  for (SlotMap::const_iterator am_i = mSlots.begin();
       am_i != mSlots.end(); am_i++) {
    for (SlotList::const_iterator sl_i = am_i->second.begin();
	 sl_i != am_i->second.end(); sl_i++) {
      SlotEntry *slot_entry = sl_i->second;
      children << slot_entry->value << slot_entry->accessor;
    }
  }
  for (VerbParasiteMap::const_iterator am_i = verb_parasites.begin();
       am_i != verb_parasites.end(); am_i++) {
    for (VerbTemplatesMap::const_iterator tl_i = am_i->second.begin();
	 tl_i != am_i->second.end(); tl_i++) {
      children << tl_i->second->definer << tl_i->second->method;
      append_datas( children, tl_i->second->argument_template );
    }
  }
  return children;
}

void Environment::set_verb_parasite( const Symbol &name,
				     unsigned int pos,
				     const var_vector &argument_template,
				     const Var &definer,
				     const Var &method ) {
  Ref<VerbDef> vd(new (aligned) VerbDef());
  vd->definer = definer;
  vd->argument_template = argument_template;
  vd->method = method;

  pair< var_vector, Ref<VerbDef> > entry(argument_template, vd );

  VerbParasiteMap::iterator nm_i = 
    verb_parasites.find( make_pair( name, pos ) );
  if ( nm_i == verb_parasites.end() ) {
    VerbTemplatesMap tmpl;
    tmpl.insert( entry );
    verb_parasites.insert( make_pair( make_pair( name, pos ), tmpl ) );
  } else {
    VerbTemplatesMap::iterator at_i =
      nm_i->second.find( argument_template );
    if (at_i != nm_i->second.end()) {
      at_i->second = vd;
    } else {
      nm_i->second.insert( entry );
    }
  }

}

void Environment::rm_verb_parasite( const Symbol &name,
				    unsigned int pos,
				    const var_vector &argument_template ) {

  VerbParasiteMap::iterator nm_i = 
    verb_parasites.find( make_pair( name, pos ) );
  
  assert( nm_i != verb_parasites.end() );

  VerbTemplatesMap::iterator tm_i = nm_i->second.find( argument_template );

  assert( tm_i != nm_i->second.end() );

  nm_i->second.erase( tm_i );
}

VerbList Environment::get_verb_parasite( const Symbol &name,
					 unsigned int pos ) const {
  VerbList results;

  VerbParasiteMap::const_iterator nm_i = 
    verb_parasites.find( make_pair( name, pos ) );
  if (nm_i == verb_parasites.end())
    return results;
  for (VerbTemplatesMap::const_iterator it = nm_i->second.begin();
       it != nm_i->second.end(); it++) {
    results.push_back( it->second );
  }

  return results;
}


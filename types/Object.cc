/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <sstream>
#include <boost/tuple/tuple.hpp>

#include "Var.hh"
#include "Ref.hh"
#include "List.hh"
#include "Object.hh"
#include "Symbol.hh"
#include "Pool.hh"
#include "Pools.hh"

#include "Ref.hh"

#include "Error.hh"
#include "Symbol.hh"
#include "Exceptions.hh"
#include "GlobalSymbols.hh"
#include "Environment.hh"


#include "logging.hh"

#include <cassert>
#include <iostream>


using namespace mica;
using namespace std;


void Object::finalize_paged_object() {
  logger.infoStream() << "Collecting unused Object pid: " << pid 
		      << " oid: " << oid 
		      << log4cpp::CategoryStream::ENDLINE;
  Pool *pool = Pools::instance.get(pid);
  pool->eject( oid );
}

Object::Object( OID i_pid, PID i_oid )
  : pid(i_pid), oid(i_oid)
{
  paged = true;;
}

Var Object::create( int pool_id,  const Ref<Object> &parent )
{
  PID pid;
  if (pool_id == -1)
    pid = Pools::instance.getDefault();
  else
    pid = pool_id;

  Pool *pool = Pools::instance.get(pid);
  Object *self = pool->new_object();

  if ((Object*)parent) 
    self->environment()->add_delegate( self, PARENT_SYM,
				       Var(parent) );

  /** This young object is dirty! (spank spank)
   */
  self->write();

  return self; 
}


Var Object::clone() const
{
  return Object::create( pid, this->asRef<Object>() );
}

var_vector Object::delegates() const {
  return environment()->delegates();
}

Environment *Object::environment() const {

  return Pools::instance.get(pid)->get_environment( oid );
}

/** Called after all mutations.
 */
void Object::write() {
  Pools::instance.get(pid)->write( oid );
}

inline SlotResult make_result( const Object *from, 
			       const SlotEntry *slot_result ) {
  SlotResult result;
  result.definer = Var(from);
  result.value = Var(slot_result->value);

  return result;
}

SlotEntry *Object::getLocal( const Var &accessor, 
			     const Symbol &name ) const {
  return environment()->getLocal( accessor, name );
} 

SlotResult Object::get( const Var &accessor, const Symbol &name ) const 
{
  /** Attempt to actually get the value.
   */
  SlotEntry *slot_result = getLocal( accessor, name );

  if (slot_result) 
    return make_result( this, slot_result );
  else
    throw E_SLOTNF;
}



Var Object::declare( const Var &accessor, const Symbol &name, const Var &value )
{
  if (environment()->getLocal( accessor, name))
    throw E_SLOTOVLD;

  Var slot(environment()->addLocal( accessor, name, value )->value);

  /** Write it to the pool
   */
  write();

  return slot;
}

Var Object::assign( const Var &accessor, const Symbol &name, 
		    const Var &value )
{
  SlotEntry *local_slot = environment()->getLocal( accessor, name );
  if (local_slot) 
    local_slot->value = value;
  else
    throw E_SLOTNF;

  /** I'm dirty now
   */
  write();

  return value;
}



void Object::remove( const Var &accessor, const Symbol &name )
{
  if (environment()->removeLocal(accessor, name))
    write();
  else
    throw E_SLOTNF;
}

Var Object::slots() const
{
  return environment()->slots();
}

bool Object::operator==( const Object &obj ) const
{
  return &obj == this;
}

bool Object::operator==( const Var &rhs ) const
{

  if ( rhs.type_identifier() != Type::OBJECT )
    return false;

  Object *x = (rhs->asType<Object*>());

  return this == x;
}


rope_string Object::rep() const
{
  SlotEntry *slot_result = getLocal( Var(const_cast<Object*>(this)), 
				     Symbol::create("title") );
  if (slot_result) {
    rope_string dstr;
    dstr.append("<object (.name: ");
    dstr.append( slot_result->value.rep() );
    dstr.append( ") >");
    return dstr;
  } else
    return "<object>";
}


Var Object::perform( const Ref<Task> &caller, const Var &args )
{
  SlotEntry *slot_entry = environment()->getLocal( Var(VERB_SYM),
						   PERFORM_SYM );
  if (slot_entry)
    return slot_entry->value.perform( caller, args );
  else
    throw E_SLOTNF;

}

/** this should only serialize the object and not the environment --
 *  that should be left to the Pool to do
 */
rope_string Object::serialize() const
{
  rope_string s_form;

  Pack( s_form, type_identifier() );

  /** Serialize the handle information
   */
  s_form.append( Pools::instance.get(pid)->poolName.serialize() );

  Pack( s_form, oid );


  return s_form;
}


bool Object::operator<( const Var &rhs ) const
{

  if ( rhs.type_identifier() != Type::OBJECT )
    return false;

  Object *x = (rhs->asType<Object*>());

  return this < x;
}

bool Object::truth() const
{
  return true;
}


Var Object::add( const Var &rhs ) const
{
  throw invalid_type("addition of Object");
}

Var Object::sub( const Var &rhs ) const
{
  throw invalid_type("subtraction of Object");
}

Var Object::mul( const Var &rhs ) const
{
  throw invalid_type("multiplication of Object");
}

Var Object::div( const Var &rhs ) const
{
  throw invalid_type("division of Object");
}

Var Object::mod( const Var &rhs ) const
{
  throw invalid_type("modulus of Object");
}

Var Object::neg() const
{
  throw invalid_type("modulus of Object");
}

Var Object::inc() const 
{
  throw invalid_type("increment of Object");
}

Var Object::dec() const
{
  throw invalid_type("decrement of Object");
}

unsigned int Object::length() const
{
  throw invalid_type("objects have no length");
}

int Object::toint() const
{
  throw invalid_type("cannot convert Object to integer");
}

float Object::tofloat() const
{
  throw invalid_type("cannot convert Object to float");
}

bool Object::isNumeric() const
{
  return false;
}

rope_string Object::tostring() const
{
  throw invalid_type("cannot convert Object to string");
}

bool Object::isObject() const
{
  return true;
}

child_set Object::child_pointers()
{
  return environment()->child_pointers();
}
void Object::set_verb_parasite( const Symbol &name,
				unsigned int pos,
				const var_vector &argument_template,
				const Var &definer,
				const Var &method ) {
  
  environment()->set_verb_parasite(name, pos, argument_template, definer,
				   method);

  write();
}

VerbList Object::get_verb_parasite( const Symbol &name,
				    unsigned int pos ) const {
  return environment()->get_verb_parasite(name, pos);
}



void Object::rm_verb_parasite( const Symbol &name,
			       unsigned int pos,
			       const var_vector &argument_template )  {
  environment()->rm_verb_parasite(name, pos, argument_template);
  write();
}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"
#include "config.h"



#include <iostream>


#include "Data.hh"
#include "Var.hh"
#include "Exceptions.hh"
#include "List.hh"
#include "GlobalSymbols.hh"
#include "MetaObjects.hh"

using namespace mica;
using namespace std;


Data::Data()
  : reference_counted(),
    guard(0xcafebabe)
{}

Data::Data( const Data &from )
  : reference_counted(),
    guard(0xcafebabe)
{}

Data::~Data()
{
  guard = 0xdeadbeef;
}

Var Data::value() const
{
  return Var(this);
}

Var Data::clone() const
{
  return Var(this);
}

bool Data::isBlock() const
{
  return false;
}

bool Data::isObject() const
{
  return false;
}

Var Data::bor( const Var &rhs ) const
{
  throw unimplemented("bitwise or unimplemented on non-numeric types");
}

Var Data::band( const Var &rhs ) const
{
  throw unimplemented("bitwise and unimplemented on non-numeric types");
}

Var Data::eor( const Var &rhs ) const
{
  throw unimplemented("bitwise and unimplemented on non-numeric types");
}

Var Data::lshift( const Var &rhs ) const
{
  throw unimplemented("bitwise left shift unimplemented on non-numeric types");
}

Var Data::rshift( const Var &rhs ) const
{
  throw unimplemented("bitwise right shift unimplemented on non-numeric types");
}

Var Data::perform( const Ref<Frame> &caller,  const Var &args )
{
  throw unimplemented("perform operation not implemented for this type");
}

void mica::writeString( mica_string &s_form, const mica_string &istr )
{
  size_t len = istr.size();
  Pack( s_form, len );
  s_form.append( istr );
}

void mica::append_data( child_set &list, const Var &var )
{
  if (var.isData())
    list.push_back( var->asType<reference_counted*>() );
}

void mica::append_datas( child_set &children,
			const var_vector &data ) {
  for (var_vector::const_iterator x = data.begin();
       x != data.end(); x++ ) {
    if (x->isData())
      children.push_back( (*x)->asType<reference_counted*>() );
  }
}

child_set mica::data_list( const var_vector &data ) {
  child_set children;
  append_datas( children, data );
  return children;
}

child_set mica::data_single( const Var &one ) {
  child_set children;
  append_data( children, one );
  return children;
}
child_set mica::data_pair( const Var &left,
			  const Var &right ) {
  child_set children;
  append_data( children, left );
  append_data( children, right );

  return children;
}

child_set mica::data_triple( const Var &one,
			    const Var &two,
			    const Var &three ) {
  child_set children;
  append_data( children, one );
  append_data( children, two );
  append_data( children, three );

  return children;
}


child_set &mica::operator <<  (child_set &children, const Var &var) {

  append_data( children, var );

  return children;
};

unsigned int Data::hash() const
{
  return (reinterpret_cast<unsigned int>(this) * 2743 + 5923);
}



var_vector Data::delegates() const {
  return MetaObjects::delegates_for( type_identifier() );
}

Var Data::declare( const Var &accessor, const Symbol &name,
		   const Var &value ) {

  throw E_PERM;
}

OptSlot Data::get( const Var &accessor, const Symbol &name ) const {

  return OptSlot();
}

Var Data::assign( const Var &accessor, const Symbol &name,
		  const Var &value ) {

  throw E_SLOTNF;
}

void Data::remove( const Var &accessor, const Symbol &name ) {

  throw E_SLOTNF;
}

Var Data::slots() const {
  
  return List::empty()
;
}


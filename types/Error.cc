/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <sstream>

#include "Scalar.hh"

#include "Var.hh"
#include "MetaObjects.hh"


#include "Error.hh"
#include "Exceptions.hh"

using namespace mica;

Error::Error( const Symbol &iErr_sym,
	      const Ref<String> &iDescription )
  : err_sym(iErr_sym),
    desc(iDescription)
{
}

Error::Error( const Error &from )
  : err_sym(from.err_sym),
    desc(from.desc)
{
}

bool Error::operator==( const Error &rhs ) const
{
  return err_sym == rhs.err_sym;
}

bool Error::operator==( const Var &rhs ) const
{
  if ( rhs.type_identifier() != Type::ERROR )
    return false;

   
  Error *x = (rhs->asType<Error *>());

  return *this == *x;
}

bool Error::operator<( const Var &rhs ) const
{

  if ( rhs.type_identifier() != Type::ERROR )
    return false;

  Error *x = (rhs->asType<Error*>());

  return (void*)this < (void*)x;
}

Var Error::add( const Var &rhs ) const
{
  throw invalid_type("addition of Error");
}

Var Error::sub( const Var &rhs ) const
{
  throw invalid_type("subtraction of Error");
}

Var Error::mul( const Var &rhs ) const
{
  throw invalid_type("multiplication of Error");
}

Var Error::div( const Var &rhs ) const
{
  throw invalid_type("division of Error");
}


Var Error::mod( const Var &rhs ) const
{
  throw invalid_type("modulus of Error");
}

Var Error::neg() const
{
  throw invalid_type("modulus of Error");
}

Var Error::inc() const
{
  throw invalid_type("increment of Error");
}

Var Error::dec() const
{
  throw invalid_type("decrement of Error");
}

unsigned int Error::length() const
{
  throw invalid_type("cannot get length of Error");
}

int Error::toint() const
{
  throw invalid_type("cannot convert Error to integer");
}

float Error::tofloat() const
{
  throw invalid_type("cannot convert Error to float");
}

bool Error::isNumeric() const
{
  return false;
}

rope_string Error::tostring() const
{
  throw invalid_type("cannot convert Error to string");
}

rope_string Error::rep() const
{
  rope_string dstr("~");
  dstr.append( err_sym.tostring() );
  if ((String*)desc) {
    dstr.push_back('(');
    dstr.append(desc->rep());
    dstr.push_back(')');
  }

  return dstr;
}

rope_string Error::serialize() const
{
  rope_string s_form;

  Pack( s_form, type_identifier() );

  /** Write the err_sym first
   */
  s_form.append( err_sym.serialize() );

  /** Then serialize the description
   */
  bool has_desc = (String*)desc;
  Pack( s_form, has_desc ); 
  if (has_desc) 
    s_form.append( desc->serialize() );

  return s_form;
}

bool Error::truth() const
{
  return false;
}


child_set Error::child_pointers()
{
  child_set children;
  if ((String*)desc)
    children.push_back( (String*)desc );
  return children;
}

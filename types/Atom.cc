/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <iostream>

#include "Scalar.hh"
#include "Var.hh"
#include "Exceptions.hh"

#include "MetaObjects.hh"

using namespace mica;

Var Scalar::subseq( int start, int length ) const {
  throw invalid_type("attempt to extract subseq from scalar operand");
}

Var Scalar::lookup( const Var &i ) const {
  throw invalid_type("attempt to lookup item inside scalar operand");
}


var_vector Scalar::for_in( unsigned int var_index,
			   const Var &block ) const

{
  throw invalid_type("attempt to iterate scalar operand");
}

var_vector Scalar::map( const Var &expr ) const {
  throw invalid_type("attempt to map scalar operand");
}

var_vector Scalar::flatten() const
{
  var_vector ops;
  ops.push_back( Var(this) );
}

child_set Scalar::child_pointers() {
  child_set none;
  return none;
}

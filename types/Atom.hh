/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef SCALAR_HH
#define SCALAR_HH

#include "Data.hh"
namespace mica {

  /** Scalar is an partially abstract type in that it
   *  implements only those behaviours that all scalars
   *  have in common.  I.e. isScalar, responses to
   *  non-scalar methods, etc.
   */
  class Scalar 
    : public Data {

  public:

    Var subseq( int start, int length ) const;

    Var lookup( const Var &i ) const;

    inline bool isScalar() const {
      return true;
    }

    var_vector for_in( unsigned int var_index,
		       const Var &block ) const;

    var_vector map( const Var &expr ) const;
    
    var_vector flatten() const;

  public:
    virtual child_set child_pointers();
  };


};

#include <iostream>
std::ostream & operator << (std::ostream &, mica::Scalar &);

#endif /* NONE_HH */

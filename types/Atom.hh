/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef ATOM_HH
#define ATOM_HH

#include "Data.hh"
namespace mica {

  /** Atom is an partially abstract type in that it
   *  implements only those behaviours that all non-
   *  Var atoms have in have in common.  I.e. isAtom, 
   *  responses to non-scalar methods, etc.
   */
  class Atom 
    : public Data {

  public:
    /** The following protocol members will throw invalid type 
     *  exceptions.
     */

    Var subseq( int start, int length ) const;
    Var lookup( const Var &i ) const;
    Var cons( const Var &el ) const;
    Var lhead() const;
    Var ltail() const;
    var_vector map( const Var &expr ) const;
    var_vector flatten() const;

  public:
    inline bool isAtom() const {
      return true;
    }

  public:
    /** Default implementation returns empty set
     */
    virtual child_set child_pointers();
  };


};

#include <iostream>
std::ostream & operator << (std::ostream &, mica::Atom &);

#endif /* NONE_HH */

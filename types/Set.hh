/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MICA_SET_HH
#define MICA_SET_HH

#include "config.h"

#include <boost/pool/pool_alloc.hpp>

#ifdef HAVE_EXT_HASH_SET
#include <ext/hash_set>
#else
#include <hash_set>
#endif

#include "hash.hh"

#include "Data.hh"
#include "collection_protocol.hh"

namespace mica {

  typedef STD_EXT_NS::hash_set< Var, hash_var, std::equal_to<Var>,
				boost::pool_allocator<Var> > var_set;

  class Set
    : public Data, 
      public collection_protocol,
      private var_set
  {
  public:
    Type::Identifier type_identifier() const { return Type::SET; }

  protected:
    Set();

    Set( const Set &from );

    Set( const var_set &from );

  public:
    // INITIAL CONSTRUCTIONS

    /** @return an empty set
     */
    inline static const Var empty() {
      static const Var inst(new (aligned) Set());
      return inst;
    }

    /** @param N single element
     *  @return set containing a single element      
     */
    static Var single( const Var &N );

    /** @param a "var_set" STL set type
     *  @return set containing the elements
     */
    static Var from_set( const var_set &from );

    /** @return a "var_set" STL conversions
     */
    var_set as_var_set() const;

  public:
    bool operator==( const Var & rhs) const;

    bool operator<( const Var& ) const;

  public:
    /** @return false if the list is empty
     */
    inline bool truth() const {
      return null();
    }

    /** @return false
     */
    inline bool isAtom() const {
      return false;
    }

    /** @return false
     */
    inline bool isNumeric() const {
      return false;
    }

    /** @return true
     */
    inline bool isSequence() const {
      return true;
    }

  public:
    // ATOM ARITHMETIC PROTOCOL

    Var add( const Var &rhs ) const;

    Var mul( const Var &rhs ) const;

    Var sub( const Var &rhs ) const;

    Var div( const Var &rhs ) const;

  public:
    // CONSTRUCTIONS

    /** add a new element to the set
     *  @param N element to add
     *  @return set containing the new element at the front
     */
    Var insert( const Var &N ) const;

    /** add a sequence of elements to the set
     *  @param N sequence of elements to add
     *  @return set containing the new element at the front
     */
    Var insertSeq( const Var &N ) const;

    /** union of two sets.  "union" is a C++ reserved word.  we use merge
     *  @param N set to merge with
     *  @return set containing the new element at the read
     */
    Var merge( const Var &N ) const;

  public:
    // DESTRUCTIONS

    /** Delete the occurence of an item in a set
     *  @param n the element to remove
     *  @return the set without the element
     */
    Var drop( const Var &n ) const;

    /** Delete the occurence of each item in a sequence from this set
     *  @param n the sequence of elements to remove
     *  @return the set without each element in the sequence
     */
    Var dropSeq( const Var &n ) const;

  public:
    //  OBSERVERS

    /** @return true if the set is empty and false otherwise
     */
    bool null() const;

    /** @return the length of the set
     */
    int size() const;

    /** Test whether the given element is in the set. 
     *  @param n member to test for
     *  @return true if the set contains member N
     */
    bool member( const Var &n ) const;

    /** Test whether a given element is in the set, 
     *  and return it.  
     *  @throw not_found if element is not found
     *  @param a the value to look for
     *  @return value equivalent to a
     */
    Var lookup( const Var &n ) const;

    /** Test whether a given element is in the set, 
     *  and return it.  Returns None if the element is not found.
     *  @param a the value to look for
     *  @return value equivalent to a or None
     */
    Var lookupM( const Var &n ) const;

    /** Test whether a given element is in the colleciton
     *  and return it.  Return a given default value if the element is
     *  not found.
     */
    Var lookup_withDefault( const Var &n, const Var &d ) const;
    
  public:
    // SET OPERATIONS

    /** Return the intersection of two sets
     *  @param a set to intersect with
     *  @return set of intersected elements
     */
    Var intersect( const Var &a ) const;

    /** Return the different between two sets
     *  @param a the set to compute difference with
     *  @return the set of all elements in the first that are not in the second
     */
    Var difference( const Var &a ) const;

    /** Test whether every element in the first set
     *  is also in the second set.
     *  @param second set to match with
     *  @return true or false if the second set contains this
     */
    bool subset( const Var &a ) const;

  public:
    mica_string rep() const;
    
    void serialize_to( serialize_buffer &s_form ) const;

    var_vector flatten() const;
   
    var_vector map( const Var &expr ) const;

  public:
    size_t hash() const;

    void append_child_pointers( child_set &child_list );    

  public:
    // INVALID OPERATIONS FOR SEQUENCES
    int toint() const;
    float tofloat() const;
    Var mod( const Var &rhs ) const;
    Var neg() const;
    Var subseq( int idx, int length ) const;
    Var cons( const Var &el ) const;
    Var lhead() const;
    Var ltail() const;
    
    mica_string tostring() const;


  };
}

#endif

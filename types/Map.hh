/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MICA_MAP_HH
#define MICA_MAP_HH

#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_map>
#else
#include <hash_map>
#endif

#include <boost/pool/pool_alloc.hpp>

#include "hash.hh"

#include "Data.hh"
#include "association_protocol.hh"

namespace mica {

  typedef STD_EXT_NS::hash_map< Var, Var, hash_var, 
				std::equal_to<Var>,
			        boost::pool_allocator<Var> > var_map;
  
  class Map
    : public Data, 
      public association_protocol,
      private var_map
  {
  public:
    Type::Identifier type_identifier() const { return Type::MAP; }

  protected:
    Map();

    Map( const Map &from );

    Map( const var_map &from );

  public:
    // INITIAL CONSTRUCTIONS

    /** @return an empty map
     */
    inline static const Var empty() {
      static const Var inst(new (aligned) Map());
      return inst;
    }

    /** @param N single element
     *  @return map containing a single element      
     */
    static Var single( const Var &N, const Var &key );

    /** @param a "var_map" STL map type
     *  @return map containing the elements
     */
    static Var from_map( const var_map &from );

    /** @return a "var_map" STL conversions
     */
    var_map as_var_map() const;

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

    /** add a new element to the association
     *  @param K key to add
     *  @param V value to add
     *  @return association containing the new association
     */
    Var insert( const Var &K, const Var &V ) const;

    /** add a sequence of elements to the association
     *  @param even lengthed sequence of paired elements
     *  @return association containing the new association
     */
    Var insertSeq( const Var &N ) const;

  public:
    // DESTRUCTIONS

    /** Delete the occurence of an item in a association
     *  @param n the element to remove
     *  @return the association without the element
     */
    Var drop( const Var &n ) const;

    /** Delete the occurence of each item in a sequence from this association
     *  @param n the sequence of elements to remove
     *  @return the association without each element in the sequence
     */
    Var dropSeq( const Var &n ) const;

  public:
    //  OBSERVERS

    /** @return true if the association is empty and false otherwise
     */
    bool null() const;

    /** @return the length of the association
     */
    int size() const;

    /** Test whether the given element is in the association. 
     *  @param n member to test for
     *  @return true if the association contains member N
     */
    bool member( const Var &n ) const;

    /** Test whether a given element is in the association, 
     *  and return its association.
     *  @throw not_found if element is not found
     *  @param a the value to look for
     *  @return value mapped to n
     */
    Var lookup( const Var &n ) const;

    /** Test whether a given element is in the association, 
     *  and return its association.  Returns None if the element
     *  is not found.
     *  @param a the value to look for
     *  @return value mapped to n or None
     */
    Var lookupM( const Var &n ) const;

    /** Test whether a given element is in the colleciton
     *  and return its association.  Return a given default value
     *  if the element is not found.
     */
    Var lookup_withDefault( const Var &n, const Var &d ) const;

  public:
    mica_string rep() const;
    
    void serialize_to( serialize_buffer &s_form ) const;

    var_vector flatten() const;
   
    var_vector map( const Var &expr ) const;

  public:
    size_t hash() const;

    void append_child_pointers( child_set &child_list );    

  public:
    // INVALID OPERATIONS FOR MAPS
    Var subseq( int, int ) const;
    Var cons( const Var &el ) const;
    Var lhead() const;
    Var ltail() const;

    int toint() const;
    float tofloat() const;
    Var mod( const Var &rhs ) const;
    Var neg() const;
    mica_string tostring() const;


  };
}

#endif

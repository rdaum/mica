/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef LIST_HH
#define LIST_HH

#include <vector>

#include "Data.hh"
#include "sequence_protocol.hh"

namespace mica {

  /** Implementation of the sequence protocol using vector<Var> --
   *  not the most efficient, but works for now.
   */
  class List
    : public Data,
      virtual sequence_protocol,   // virtual, it's just a damn interface
      private var_vector           // private so we can't be messed with
  {
    /** Really, a large part of the implementation can probably be shared
     *  with String.  They should inherit some common methods from
     *  a templated ancestor.  
     */
  public:
    Type::Identifier type_identifier() const { return Type::LIST; }; 

  protected:
    List();

    List( const var_vector &from );

  public:
    bool operator==( const Var & rhs) const;

    bool operator<(const Var&) const;

  public:
    // CONSTRUCTIONS

    /** Return the same empty list of and over again, instead of
     *  constructing a new list each time (since lists are immutable
     *  anyways, this means fast comparisons for empty lists.)
     */
    static inline const Var empty() {
      static const Var inst(new (aligned) List());
      return inst;
    }

    static Var single( const Var &el );
    static Var tuple( const Var &l, const Var &r );
    static Var triple( const Var &one, const Var &two, const Var &three );

    /** Build a list from a vector of vars.  This is a static
     *  function instead of a constructor so we can control its
     *  usage in some tricky ways.  I.e. if the vector is empty,
     *  we use empty(), instead.  This makes room for memoization
     *  or hash flattening in the future.
     */
    static Var from_vector( const var_vector &from );

    /** To get access to this as a vector, you have to make a copy
     *  first.  This is to protect it from mutation.
     */
    var_vector as_vector() const;

    /** add a new element to the front/left of a sequence
     *  @param N element to add
     *  @return sequence containing the new element at the front
     */
    Var cons( const Var &el ) const;

    /** add a new element to the right/read of a sequence
     *  @param N element to add
     *  @return sequence containing the new element at the read
     */
    Var snoc( const Var &el ) const;

    /** append two sequences to create one sequence
     *  @param N sequence to append to this sequence
     *  @return sequence with N appended
     */
    Var append( const Var &seq ) const;


  public:
    // SCALAR ARITHMETIC PROTOCOL

    Var add( const Var &rhs ) const;

    Var mul( const Var &rhs ) const;

    Var sub( const Var &rhs ) const; 

    Var div( const Var &rhs ) const;

  public:
    /** @return false if the list is empty
     */
    inline bool truth() const {
      return null();
    }

    /** @return false
     */
    inline bool isScalar() const {
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
    // DESTRUCTIONS

    /** @return sequence separated into its first element and the remaining
     *          sequence
     */
    Var lview() const;

    /** @return the first element of the sequence.
     *  @throws out_of_range if sequence is empty
     */
    Var lhead() const;

    /** @return sequence minus the first element.  returns empty() if the
     *          sequence is already empty
     */
    Var ltail() const;

    /** @return sequence separated into its last element and the reamining
     *          sequence.  Returns None if the sequence is empty
     */
    Var rview() const;

    /** @return return the last element of the sequence. 
     *  @throws out_of_range if the sequence is empty
     */
    Var rhead() const;

    /** @return sequence without the last element.  empty if the sequence
     *          is empty
     */
    Var rtail() const;

  public:
    //  OBSERVERS

    /** @return true if the sequence is empty and false otherwise
     */
    bool null() const;

    /** @return the length of the sequence
     */
    int size() const;

  public:
    //  CONCAT AND REVERSE

    /** @return a sequence of sequences flattened into a simple sequence
     */
    Var concat() const;

    /** @return the sequence in reverse order
     */
    Var reverse() const;

  public:
    // SUBSEQUENCES

    /** extract a prefix from the sequence. 
     *  @param i length of the prefix sequence
     *  @return prefix of length i, or empty if i is negative, or 
     *          entire sequence if i is too large
     */
    Var take( int i ) const;

    /** drop a prefix of length i from the sequence
     *  @param i length of prefix sequence
     *  @return sequence without prefix of length i, or empty if i
     *          is too large, or entire sequence if i is negative
     */
    Var drop( int i ) const;

    /** Split the sequence into a prefix of length i, and the remaining 
     *  sequence.  Behaves the same as corresponding calls to take and drop if
     *  length is too large or negative
     *  @param i length of prefix
     *  @return pair of sequences split at prefix
     */
    Var splitAt( int i ) const;

    /** Extract a subsequence from a sequence.  
     *  @param start start index of the subsequence
     *  @param length length of the subsequence
     *  @return subsequence, or empty if start is negative, or entire
     *          remainder if length is too large
     */
    Var subseq( int start, int length ) const;

  public:
    // INDEX-BASED OPERATIONS
    // All operations assume zero-based indexing.

    /** Test whether an index is valid for a given sequence
     *  @param i index
     *  @return true if index is valid false if not
     */
    bool inBounds( int i ) const;

    /** @param i index to retrieve an element from
     *  @return the element at the given index.  
     *  @throw out_of_range if the index is out of bounds
     */
    Var lookup( const Var &i ) const;

    /** @param i index to retrieve an element from
     *  @return the element at the given index, or None if index is invalid
     */
    Var lookupM( int i ) const;

    /** @param i index to retrieve an element from
     *  @param d default to return if the index is not found
     *  @return the element at the given index.  
     */
    Var lookup_withDefault( int i, const Var &d ) const;

    /** Return the sequence with the element at the given index replaced
     *  @param i index to replace at
     *  @param e element to replace with
     *  @return sequence with element replaced, or original sequence if 
     *          index is out of bounds
     */
    Var update( int i, const Var &e ) const;

  public:
    // ZIPS AND UN-ZIPS

    /** Combine two sequences into a sequence of pairs.  If the sequences
     *  are of different lengths, the excess elements of the longer sequence
     *  are discarded
     *  @param rhs second sequence
     *  @return the sequences combined
     */
    Var zip( const Var &with ) const;

    /** Combine three sequences into a sequence of triples.  If the sequences
     *  are of different lengths, the excess elements of the longer sequence
     *  are discarded
     *  @param second second sequence
     *  @param third third sequence
     *  @return the sequences combined
     */
    Var zipTriple( const Var &one, const Var& two ) const;

    /** Transpose a sequence of pairs into a pair of sequences
     *  @return pair of unzipped sequences
     */
    Var unzip() const;

    /** Transpose a sequence of triples into three sequences
     *  @return triple of unzipped sequences
     */
    Var unzipTriple() const;

  public:
    mica_string rep() const;

    mica_string serialize() const;

  public:
    var_vector for_in( unsigned int var_index,
		       const Var &block ) const;

    var_vector map( const Var &expr ) const;
 
    var_vector flatten() const;

  public:
    size_t hash() const;

    child_set child_pointers();

  public:
    // INVALID OPERATIONS FOR SEQUENCES
    int toint() const;
    float tofloat() const;
    Var mod( const Var &rhs ) const; 
    Var neg() const;
    mica_string tostring() const;
    
  };
}

#endif

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef STRING_HH
#define STRING_HH

#include "rope_string.hh"

#include "Data.hh"
#include "sequence_protocol.hh"

namespace mica {

  class String 
    : public Data, 
      virtual sequence_protocol,
      private mica_string
  {
  public:
    Type::Identifier type_identifier() const { return Type::STRING; }

  protected:
    String();

    virtual ~String() {};

    String( const mica_string &from );

    String( const char *c_str );

    String( const String &from );

  public:
    bool operator==( const Var & rhs) const;

    bool operator<(const Var&) const;

  public:
    // CONSTRUCTIONS

    /** Return the same empty string of and over again, instead of
     *  constructing a new string each time (since strings are immutable
     *  anyways, this means fast comparisons for empty string.)
     */
    static inline const Var empty() {
      static const Var inst(new (aligned) String());
      return inst;
    }

    static Ref<String> create( const char *c_str );

    /** Build a string from a rope string.  This is a static
     *  function instead of a constructor so we can control its
     *  usage in some tricky ways.  I.e. if the vector is empty,
     *  we use empty(), instead.  This makes room for memoization
     *  or hash flattening in the future.
     */
    static Var from_rope( const mica_string &from );

    /** As above, but from a C-style null-terminated string
     */
    static Var from_cstr( const char *c_str );

    /** To get access to this as a rope, you have to make a copy
     *  first.  This is to protect it from mutation.
     */
    mica_string as_rope() const;

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
    // ATOM ARITHMETIC PROTOCOL

    Var add( const Var &rhs ) const;

    Var mul( const Var &rhs ) const;

    Var sub( const Var &rhs ) const; 

    Var div( const Var &rhs ) const;

  public:
    /** @return false if the string is empty
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
    // ZIPS AND UN-ZIPS - invalid in strings

    Var zip( const Var &with ) const;
    Var zipTriple( const Var &two, const Var &three ) const;
    Var unzip() const;
    Var unzipTriple() const;

  public:
    mica_string tostring() const;

    mica_string rep() const ;

    mica_string serialize() const;

    var_vector flatten() const;

    var_vector map( const Var &expr ) const;

  public:
    child_set child_pointers() { return child_set(); } 

    size_t hash() const;

  public:
    // INVALID OPERATIONS FOR SEQUENCES
    int toint() const;
    float tofloat() const;
    Var mod( const Var &rhs ) const; 
    Var neg() const;
  };     
};


#endif /* STRING_HH */

#ifndef MICA_SEQUENCE_PROTOCOL_HH
#define MICA_SEQUENCE_PROTOCOL_HH

namespace mica {

  /** Protocol for purely functional (immutable) sequences.  Based roughly
   *  on the API from Edison for Haskell by Chris Okasaki.  
   */
  class sequence_protocol
  {
  public:
    // INITIAL CONSTRUCTIONS

    // The following static functions should be defined by all
    //  sequences and should be used in place of C++ constructors.
    //

    /** @return an empty sequence
     */
    //    static Var empty() const = 0;
  
    /** @param N single element
     *  @return sequence containing a single element      
     */
    //    static Var single( const Var &N ) const = 0;

  public:
    // CONSTRUCTIONS

    // THIS FUNCTION NOW AVAILABLE VIA type_protocol

    //     /** add a new element to the front/left of a sequence
    //      *  @param N element to add
    //      *  @return sequence containing the new element at the front
    //      */
    //     virtual Var cons( const Var &el ) const = 0;

    /** add a new element to the right/read of a sequence
     *  @param N element to add
     *  @return sequence containing the new element at the read
     */
    virtual Var snoc( const Var &el ) const = 0;

    /** append two sequences to create one sequence
     *  @param N sequence to append to this sequence
     *  @return sequence with N appended
     */
    virtual Var append( const Var &seq ) const = 0;

  public:
    // DESTRUCTIONS

    /** @return sequence separated into its first element and the remaining
     *          sequence
     */
    virtual Var lview() const = 0;

    // THESE TWO FUNCTIONS NOW HANDLED ON type_protocol

    //     /** @return the first element of the sequence.
    //      *  @throws out_of_range if sequence is empty
    //      */
    //     virtual Var lhead() const = 0;
    
    //     /** @return sequence minus the first element.  returns empty() if the
    //      *          sequence is already empty
    //      */
    //     virtual Var ltail() const = 0;

    /** @return sequence separated into its last element and the reamining
     *          sequence.  Returns None if the sequence is empty
     */
    virtual Var rview() const = 0;

    /** @return return the last element of the sequence. 
     *  @throws out_of_range if the sequence is empty
     */
    virtual Var rhead() const = 0;

    /** @return sequence without the last element.  empty if the sequence
     *          is empty
     */
    virtual Var rtail() const = 0;

  public:
    //  OBSERVERS

    /** @return true if the sequence is empty and false otherwise
     */
    virtual bool null() const = 0;

    /** @return the length of the sequence
     */
    virtual int size() const = 0;

  public:
    //  CONCAT AND REVERSE

    /** @return a sequence of sequences flattened into a simple sequence
     */
    virtual Var concat() const = 0;

    /** @return the sequence in reverse order
     */
    virtual Var reverse() const = 0;

  public:
    // SUBSEQUENCES

    /** extract a prefix from the sequence. 
     *  @param i length of the prefix sequence
     *  @return prefix of length i, or empty if i is negative, or 
     *          entire sequence if i is too large
     */
    virtual Var take( int i ) const = 0;

    /** drop a prefix of length i from the sequence
     *  @param i length of prefix sequence
     *  @return sequence without prefix of length i, or empty if i
     *          is too large, or entire sequence if i is negative
     */
    virtual Var drop( int i ) const = 0;

    /** Split the sequence into a prefix of length i, and the remaining 
     *  sequence.  Behaves the same as corresponding calls to take and drop if
     *  length is too large or negative
     *  @param i length of prefix
     *  @return pair of sequences split at prefix
     */
    virtual Var splitAt( int i ) const = 0;

    /** Extract a subsequence from a sequence.  
     *  @param start start index of the subsequence
     *  @param length length of the subsequence
     *  @return subsequence, or empty if start is negative, or entire
     *          remainder if length is too large
     */
    virtual Var subseq( int start, int length ) const = 0;

  public:
    // INDEX-BASED OPERATIONS
    // All operations assume zero-based indexing.

    /** Test whether an index is valid for a given sequence
     *  @param i index
     *  @return true if index is valid false if not
     */
    virtual bool inBounds( int i ) const = 0;

    /** @param i index to retrieve an element from
     *  @return the element at the given index.  
     *  @throw out_of_range if the index is out of bounds
     */
    virtual Var lookup( const Var &i ) const = 0;

    /** @param i index to retrieve an element from
     *  @return the element at the given index, or None if index is invalid
     */
    virtual Var lookupM( int i ) const = 0;

    /** @param i index to retrieve an element from
     *  @param d default to return if the index is not found
     *  @return the element at the given index.  
     */
    virtual Var lookup_withDefault( int i, const Var &d ) const = 0;

    /** Return the sequence with the element at the given index replaced
     *  @param i index to replace at
     *  @param e element to replace with
     *  @return sequence with element replaced, or original sequence if 
     *          index is out of bounds
     */
    virtual Var update( int i, const Var &e ) const = 0;

  public:
    // ZIPS AND UN-ZIPS

    /** Combine two sequences into a sequence of pairs.  If the sequences
     *  are of different lengths, the excess elements of the longer sequence
     *  are discarded
     *  @param rhs second sequence
     *  @return the sequences combined
     */
    virtual Var zip( const Var &rhs ) const = 0;

    /** Combine three sequences into a sequence of triples.  If the sequences
     *  are of different lengths, the excess elements of the longer sequence
     *  are discarded
     *  @param second second sequence
     *  @param third third sequence
     *  @return the sequences combined
     */
    virtual Var zipTriple( const Var &second, const Var &third ) const = 0;

    /** Transpose a sequence of pairs into a pair of sequences
     *  @return pair of unzipped sequences
     */
    virtual Var unzip() const = 0;

    /** Transpose a sequence of triples into three sequences
     *  @return triple of unzipped sequences
     */
    virtual Var unzipTriple() const = 0;
  };

} /** namespace mica ... **/

#endif

#ifndef MICA_COLLECTION_PROTOCOL_HH
#define MICA_COLLECTION_PROTOCOL_HH

namespace mica {

  /** Protocol for purely functional (immutable) sets.  Based roughly
   *  on the API from Edison for Haskell by Chris Okasaki.  
   */
  class collection_protocol
  {
  public:
    // INITIAL CONSTRUCTIONS

    // The following static functions should be defined by all
    //  collections and should be used in place of C++ constructors.
    //

    /** @return an empty collection
     */
    //    static Var empty() const = 0;
  
    /** @param N single element
     *  @return collection containing a single element      
     */
    //    static Var single( const Var &N ) const = 0;

  public:
    // CONSTRUCTIONS

    /** add a new element to the collection
     *  @param N element to add
     *  @return collection containing the new element at the front
     */
    virtual Var insert( const Var &N ) const = 0;

    /** add a sequence of elements to the collection
     *  @param N sequence of elements to add
     *  @return collection containing the new element at the front
     */
    virtual Var insertSeq( const Var &N ) const = 0;

    /** union of two collections.  union is a reserved word,
     *  so we use "merge" :-(
     *  @param N collection to merge with
     *  @return collection containing the new element at the read
     */
    virtual Var merge( const Var &N ) const = 0;

  public:
    // DESTRUCTIONS

    /** Delete the occurence of an item in a collection
     *  @param n the element to remove
     *  @return the set without the element
     */
    virtual Var drop( const Var &n ) const = 0;

    /** Delete the occurence of each item in a sequence from this collection
     *  @param n the sequence of elements to remove
     *  @return the set without each element in the sequence
     */
    virtual Var dropSeq( const Var &n ) const = 0;

  public:
    //  OBSERVERS

    /** @return true if the collection is empty and false otherwise
     */
    virtual bool null() const = 0;

    /** @return the length of the collection
     */
    virtual int size() const = 0;

    /** Test whether the given element is in the collection. 
     *  @param n member to test for
     *  @return true if the set contains member N
     */
    virtual bool member( const Var &n ) const = 0;

    /** Test whether a given element is in the collection, 
     *  and return it.  
     *  @throw not_found if element is not found
     *  @param a the value to look for
     *  @return value equivalent to a
     */
    virtual Var lookup( const Var &n ) const = 0;

    /** Test whether a given element is in the collection, 
     *  and return it.  Returns None if the element is not found.
     *  @param a the value to look for
     *  @return value equivalent to a or None
     */
    virtual Var lookupM( const Var &n ) const = 0;

    /** Test whether a given element is in the colleciton
     *  and return it.  Return a given default value if the element is
     *  not found.
     */
    virtual Var lookup_withDefault( const Var &n, const Var &d ) const = 0;
    
  public:
    // SET OPERATIONS

    /** Return the intersection of two collections
     *  @param a collection to intersect with
     *  @return collection of intersected elements
     */
    virtual Var intersect( const Var &a ) const = 0;

    /** Return the different between two collections
     *  @param a the collection to compute difference with
     *  @return the set of all elements in the first that are not in the second
     */
    virtual Var difference( const Var &a ) const = 0;

    /** Test whether every element in the first collection
     *  is also in the second collection.
     *  @param second collection to match with
     *  @return true or false if the second collection contains this
     */
    virtual bool subset( const Var &a ) const = 0;

  };

} /** namespace mica ... **/

#endif

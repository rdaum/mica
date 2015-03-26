#ifndef MICA_ASSOC_PROTOCOL_HH
#define MICA_ASSOC_PROTOCOL_HH

namespace mica {

/** Protocol for purely functional (immutable) sets.  Based roughly
 *  on the API from Edison for Haskell by Chris Okasaki.
 */
template <typename T>
class association_protocol {
 public:
  // INITIAL CONSTRUCTIONS

  // The following static functions should be defined by all
  //  associations and should be used in place of C++ constructors.
  //

  /** @return an empty association
   */
  //    static T empty() const = 0;

  /** @param N single element
   *  @return association containing a single element
   */
  //    static T single( const T &K, const T V ) const = 0;

 public:
  // CONSTRUCTIONS

  /** add a new element to the association
   *  @param K key to add
   *  @param V value to add
   *  @return association containing the new association
   */
  virtual T insert(const T &K, const T &V) const = 0;

  /** add a sequence of elements to the association
   *  @param even lengthed sequence of paired elements
   *  @return association containing the new association
   */
  virtual T insertSeq(const T &N) const = 0;

 public:
  // DESTRUCTIONS

  /** Delete the occurence of an item in a association
   *  @param n the element to remove
   *  @return the association without the element
   */
  virtual T drop(const T &n) const = 0;

  /** Delete the occurence of each item in a sequence from this association
   *  @param n the sequence of elements to remove
   *  @return the association without each element in the sequence
   */
  virtual T dropSeq(const T &n) const = 0;

 public:
  //  OBSERVERS

  /** @return true if the association is empty and false otherwise
   */
  virtual bool null() const = 0;

  /** @return the length of the association
   */
  virtual int size() const = 0;

  /** Test whether the given element is in the association.
   *  @param n member to test for
   *  @return true if the association contains member N
   */
  virtual bool member(const T &n) const = 0;

  /** Test whether a given element is in the association,
   *  and return its association.
   *  @throw not_found if element is not found
   *  @param a the value to look for
   *  @return value mapped to n
   */
  virtual T lookup(const T &n) const = 0;

  /** Test whether a given element is in the association,
   *  and return its association.  Returns None if the element
   *  is not found.
   *  @param a the value to look for
   *  @return value mapped to n or None
   */
  virtual T lookupM(const T &n) const = 0;

  /** Test whether a given element is in the colleciton
   *  and return its association.  Return a given default value
   *  if the element is not found.
   */
  virtual T lookup_withDefault(const T &n, const T &d) const = 0;
};

}  // namespace mica

#endif  // MICA_ASSOC_PROTOCOL_HH

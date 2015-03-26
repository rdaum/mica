/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef MICA_TEMPL_VAR_HH
#define MICA_TEMPL_VAR_HH

#include <typeinfo>

#include "base/strings.hh"
#include "base/types.hh"

namespace mica {

/** A reference counting smart pointer with support for multiple
 *  type storage and an interface for a common data manipulation
 *  protocol.
 */
template <class T>
class Ref {
  friend class Var;  // Var constructor accesses data directly
 protected:
  T *data;

 public:
  /** construct a copy of a Ref from another Ref
   *  @param from what to copy from
   */
  Ref<T>(const Ref<T> &from) : data(from.data) { upcount(); }

  /** construct a Ref with initial value
   *  @param initial value of the Ref
   */
  Ref<T>(T *initial) : data(initial) { upcount(); }

  /** Destructor which is called when a Ref is deleted or goes out of
   *  scope.
   *  deleting a Ref reduces the reference count on whatever the
   *  Ref was pointing to.
   */
  ~Ref() { dncount(); }

 public:
  /** Return the C++ RTTI typeid of what is being held.
   */
  const std::type_info &typeOf() const { return typeid(T); }

  Type::Identifier type_identifier() const { return data->type_identifier(); }

  /** Cast a pointer out of a Ref.
   *  @return the pointer held in the Ref, after a type check
   */
  operator T *() const { return data; }

  /** Dereference into Data from Ref.
   */
  T *operator->() const { return data; }

 public:
  /** assignment operator
   */
  Ref<T> &operator=(const Ref<T> &f) {
    if (this == &f)
      return *this;

    // first dncount whatever was there before
    dncount();

    data = f.data;

    // now upcount new value
    upcount();

    return *this;
  }

  /** assignment to Data
   */
  Ref &operator=(T *rhs) {
    // first dncount whatever was there before
    dncount();

    data = rhs;

    upcount();

    return *this;
  }

  /** equivalence comparison operator
   *  @param v2 right hand side of comparison
   *  @return truth value of comparison
   */
  bool operator==(const Ref<T> &v2) const {
    if (&v2 == this)
      return true;

    return data == v2.data || (v2.data && data && (data->operator==(v2.data)));
  }

  /** less-than comparison
   *  @param v2 right hand side of comparison
   *  @return truth value of comparison
   */
  bool operator<(const Ref<T> &v2) const { return data->operator<(v2.data); }

  /** greater-than comparison
   *  @param v2 right hand side of comparison
   *  @return truth value of comparison
   */
  bool operator>(const Ref<T> &v2) const { return data->operator>(v2.data)(); }

  /** less-than-or-equal comparison
   *  @param v2 right hand side of comparison
   *  @return truth value of comparison
   */
  bool operator<=(const Ref<T> &v2) const { return data->operator<=(v2.data); }

  /** great-than-or-equal comparison
   *  @param v2 right hand side of comparison
   *  @return truth value of comparison
   */
  bool operator>=(const Ref<T> &v2) const { return data->operator>(v2.data); }

 public:
  /** return truth value of value
   */
  bool truth() const { return data->truth(); }

  /** @return truth value of the Ref
   */
  bool operator!() const { return !truth(); }

 public:
  /** Convert to string.
   */
  mica_string tostring() const { return data->tostring(); }

  /** A printable representation
   */
  mica_string rep() const { return data->rep(); }

  /** Append as a string to an ostream.  Used by operator<<(ostream).
   */
  std::ostream &append(std::ostream &lhs) const {
    lhs << rep();
    return lhs;
  }

 public:
  /** increment reference count on thing pointed to
   *  @return copy of this
   */
  inline void upcount() {
    if (data)
      data->upcount();
  }

  /** decrement reference count on thing pointed to
   *  @return copy of this
   */
  inline void dncount() {
    if (data)
      data->dncount();
  }

 public:
  unsigned int hash() const {
    if (data)
      return data->hash();
    else
      return 0;
  }
};

}  // namespace mica

#endif  // MICA_TEMPL_VAR_HH

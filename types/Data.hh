/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MICA_DATA_HH
#define MICA_DATA_HH

#include <iostream>

#include <list>
#include <vector>

#include <boost/function.hpp> 


#include <stdexcept>

#include "reference_counted.hh"
#include "type_protocol.hh"
#include "aligned_allocator.hh"

#include "Ref.hh"

namespace mica {
 
  /** Used to point to a binary function
   */
  typedef boost::function<Var (const Var &, const Var &)> BinOpFunc;

  /** Used for binary reduction function
   */
  typedef Var (*BinaryVarFunction)(const Var &, const Var &r) ;

  /** Mostly abstract class inheriting reference counting and
   *  the type protocol, plus:
   *         guard + assertions/invariants to check runtime object validity
   *         some convenience stub implementations of some protocol
   *              methods
   *         functions for outputting to iostreams, appending to lists, etc.
   */
  class Data 
    : public reference_counted, public type_protocol
  {
  public:
    unsigned int guard;

  public:
    /** constructor
     */
    Data();

    /** copy constructor
     */
    Data( const Data &from );

    /** invariant that is assuredly static, intended to deal with
     * the possibility of dangling pointers, in which case any 
     * virtual invariants will not behave properly.  Currently
     * invariants are not virtual, so this is somewhat redundant.
     */

    inline static bool static_invariant(const Data *instance) {
      return instance->guard == 0xcafebabe;
    }
	  
    /** class invariant.
     */
    inline bool invariant() const { 
      return Data::static_invariant(this);
    }

    /** destructor
     */
    virtual ~Data();

  public:
    /** Return a typed reference counted pointer to this version
     *  -- a lighter, type-checked alternative to using Var.
     */
    template<class T> Ref<T> asRef() const {
      return Ref<T>( (T*)this );
    }

    template<class T> T asType() const {
      return dynamic_cast<T>(const_cast<Data *>(this));
    }

  public:

    template<typename R, typename T>
    R apply_dynamic_visitor( const T &x ) const {
      return x.operator()( *this );
    }
    
  public:
    
    /** Return the prototypes that his object delegates to during
     *  dispatch / slot-lookup
     */
    virtual var_vector delegates() const;

    /** return true value of this.  usually just returns
     *  "this", but in case of Iterators and other magic types
     *  a sort of redirection can be accomplished.
     */
    virtual Var value() const;

    /** Return child of this object -- in all cases except Objects
     *  this is just a reference to self.
     */
    virtual Var clone() const;

  public:
    /** bitwise and  (Var operator &)
     *  @param rhs right hand side
     *  @return result of addition
     */
    virtual Var band( const Var &rhs ) const;

    /** bitwise or  (Var operator |)
     *  @param rhs right hand side
     *  @return result of addition
     */
    virtual Var bor( const Var &rhs ) const;

    /** bitwise left shift (operator <<)
     *  @param rhs right hand side
     *  @return result of addition
     */
    virtual Var lshift( const Var &rhs ) const;

    /** bitwise right shift (operator >>)
     *  @param rhs right hand side
     *  @return result of addition
     */
    virtual Var rshift( const Var &rhs ) const;

    /** exclusive or operation
     *  @param rhs right hand side of exclusive or
     *  @return result
     */
    virtual Var eor( const Var &rhs ) const;

   
  public:
    /** is this a method?
     */
    virtual bool isBlock() const;

    /** is this an object
     */
    virtual bool isObject() const;

  public:
    /** Declare a slot on an object
     *  @param the accessor of the slot (object, #METHOD, or #NAME)
     *  @param name of the slot to declare
     *  @return the Slot value
     */
    virtual Var declare( const Var &accessor, const Symbol &name,
                               const Var &value );

    /** Search for a slot by accessor and name
     *  @param the accessor of the slot (object, #METHOD, or #NAME)
     *  @param name the symbol to search for
     *  @return copy of value
     *  @throws slot_not_found
     */
    virtual SlotResult get( const Var &accessor, 
			    const Symbol &name ) const;

    /** assign a value to a slot
     *  @param the accessor of the slot (object, #METHOD, or #NAME)
     *  @param name of the slot to get
     *  @param value the value to set the slot to
     *  @return copy of value
     *  @throws slot_not_found
     */
    virtual Var assign( const Var &accessor, const Symbol &name,
                              const Var &value );

    /** remove a slot from this object
     *  @param the accessor of the slot (object, #METHOD, or #NAME)
     *  @param name of the slot to remove
     */
    virtual void remove( const Var &accessor, const Symbol &name );

    /** get a list of slots on this object
     */
    virtual Var slots() const;

  public:
    /** FUNCTIONAL
     */

    /** Invoke this object with arguments.  
     */
    virtual Var perform( const Ref<Task> &caller, const Var &args );

  public:

    virtual unsigned int hash() const;
  };

  void writeString( rope_string &s_form, const rope_string &istr );

  typedef void (Data::*data_member_pointer)();

  /** Utilities for creating child lists from Var smart pointers.
   */
  child_set &operator << (child_set &, 
			  const Var &);

  void append_data( child_set &children, const Var &var );
  void append_datas( child_set &children,
		     const var_vector &one );
  child_set data_list( const var_vector &data );
  child_set data_single( const Var &var );
  child_set data_pair( const Var &left, const Var &right );
  child_set data_triple( const Var &one, const Var &two,
					const Var &three);

};





#endif /* DATA_HH */

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MICA_TYPE_PROTOCOL_HH
#define MICA_TYPE_PROTOCOL_HH

#include <iostream>
#include <vector>

#include <boost/operators.hpp>

#include "aligned_allocator.hh"
#include "rope_string.hh"
#include "types.hh"
#include "Ref.hh"


namespace mica {

  /** Forward declaration of the variant storage object.
   */
  class Var;
  class Symbol;

  /** A vector of Var, for storage of variant elements.
   */
  typedef std::vector<Var> var_vector;

  struct SlotResult;

  class Task;
  class Symbol;

  class type_protocol
    : public has_type_identifier,
      public boost::totally_ordered<Var>
  {
  public:
    // CONSTRUCTION

    /** Build and return a deep copy of this item.
     */
    virtual Var clone() const = 0;

  public:
    // OBSERVATION

    /** Compare two pieces of type_protocol
     *  @param rhs right hand side of comparison
     *  @return truth value of comparison
     */
    virtual bool operator==( const Var &rhs ) const = 0;
  
    /** Return the truth value of this value
     */
    virtual bool truth() const = 0;
  
    /** Less-than comparison between two values
     *  @param v2 right hand side of comparison
     *  @return truth value of less-than ordering
     */
    virtual bool operator<( const Var &v2 ) const = 0;

    /** Get a list of prototypes that this values delegates behaviours
     *  to.
     *  @return list of delegates
     */
    virtual var_vector delegates() const = 0;

  public:
    // ARITHMETIC
  
    /** add two pieces of type_protocol together (binary operator +)
     *  @param rhs right hand side of addition
     *  @return result of addition
     */
    virtual Var add( const Var &rhs ) const = 0;
  
    /** divide by piece of type_protocol (binary operator /)
     *  @param rhs right hand side of division
     *  @return result of division
     */
    virtual Var div( const Var &rhs ) const = 0;
  
    /** multiply two pieces of type_protocol (binary operator *)
     *  @param rhs right hand side of multiplication
     *  @return result of multiplication
     */
    virtual Var mul( const Var &rhs ) const = 0;

    /** subtract two pieces of type_protocol (binary operator -)
     *  @param rhs right hand side of subtraction
     *  @return result of subtraction
     */
    virtual Var sub( const Var &rhs ) const = 0;
  
    /** mod two pieces of type_protocol (binary operator %)
     *  @param rhs right hand side of modulus
     *  @return result of mod
     */
    virtual Var mod( const Var &rhs ) const = 0;

    /** negate this and return result (unary operator -)
     *  @return result of negation
     */
    virtual Var neg() const = 0;

  
  public:
    // BITWISE OPERATIONS

    /** bitwise and (binary operator &)
     *  @param rhs right hand side
     *  @return result of bitwise and
     */
    virtual Var band( const Var &rhs ) const = 0;

    /** bitwise or (binary operator |)
     *  @param rhs right hand side
     *  @return result of bitwise or
     */
    virtual Var bor( const Var &rhs ) const = 0;

    /** bitwise left shift (binary operator <<)
     *  @param rhs right hand side
     *  @return result of bitwise left shift
     */
    virtual Var lshift( const Var &rhs ) const = 0;

    /** bitwise right shift (binary operator >>)
     *  @param rhs right hand side
     *  @return result of bitwise right shift
     */
    virtual Var rshift( const Var &rhs ) const = 0;
  
    /** bitwise exclusive or operation (binary ^)
     *  @param rhs right hand side of exclusive or
     *  @return result of bitwise eor
     */
    virtual Var eor( const Var &rhs ) const = 0;

  public:
    // SLICING

    /** Extract a subsequence from a sequence.  
     *  @param start start index of the subsequence
     *  @param length length of the subsequence
     *  @return subsequence, or empty if start is negative, or entire
     *          remainder if length is too large
     */
    virtual Var subseq( int start, int length ) const = 0;

    /** @param i index to retrieve an element from
     *  @return the element at the given index.  
     *  @throw out_of_range if the index is out of bounds
     */
    virtual Var lookup( const Var &i ) const = 0;

  public:
    // TRAMPOLINES
  
    /** Return result of applying an expression for each element contained
     *  in this item, and pushing the result to the stack.
     *  @param expr opcode expression to apply for each iteration
     *  @throw invalid_type for non-iteratable types
     *  @return trampoline of opcodes for pushing to the VM
     */
    virtual var_vector map( const Var &expr ) const = 0;

    /** Return result of iterating each element contained in this item
     *  and assigning them to a variable, then executing a block
     *  @param var var stack index of variable to assign into
     *  @param block block to execute on each iteration
     *  @throw invalid_type for non-iteratable types
     *  @return trampoline of opcodes for pushing to the VM
     */
    virtual var_vector for_in( unsigned int var, 
			       const Var &block ) const = 0;


    /** Return operations for flattening this object into 
     *  a surrounding stack.
     *  @return trampoline of opcodes for pushing to the VM
     */
    virtual var_vector flatten() const = 0;

  public:
    // SLOT-ASSOCIATIVE
  
    /** Declare a slot on an object
     *  @param the accessor of the slot, or None if public
     *  @param name the symbol to create
     *  @return the Slot value
     */
    virtual Var declare( const Var &accessor, const Symbol &name,
			 const Var &value ) = 0;
  
    /** Search for a slot by accessor and name
     *  @param the accessor of the slot, or None if public
     *  @param name the symbol to search for
     *  @return copy of value
     *  @throws slot_not_found
     */
    virtual SlotResult get( const Var &accessor,
			    const Symbol &name ) const = 0;

    /** assign a value to a slot
     *  @param accessor the accessor of the slot, or None if public
     *  @param name the symbol to set
     *  @param value the value to set the slot to
     *  @return copy of value
     *  @throws slot_not_found
     */
    virtual Var assign( const Var &accessor, const Symbol &name,
			const Var &value ) = 0;

    /** remove a slot
     */
    virtual void remove( const Var &accessor,  const Symbol &name ) = 0;

    /** get a list of slots
     */
    virtual Var slots() const = 0;

  public:
    // CONVERSIONS
    /** coerce this type_protocol to an integer value
     */
    virtual int toint() const = 0;

    /** coerce this type_protocol to a float value
     */
    virtual float tofloat() const = 0;

    /** return a string conversion
     */
    virtual rope_string tostring() const = 0;

    /** return a string representation
     */
    virtual rope_string rep() const = 0;

    /** return a serialization
     */
    virtual rope_string serialize() const = 0;

    /** is this a numeric type?
     */
    virtual bool isNumeric() const = 0;

    /** is this an atomic type?
     */
    virtual bool isScalar() const = 0;

    /** is this a method?
     */
    virtual bool isBlock() const = 0;

    /** is this an object
     */
    virtual bool isObject() const = 0;
  public:
    /** FUNCTIONAL
     */

    /** Invoke this object with arguments.
     */
    virtual Var perform( const Ref<Task> &caller, const Var &args ) = 0;

  public:
  
  
    virtual unsigned int hash() const = 0;
  };

};


#endif /* TYPE_PROTOCOL_HH */

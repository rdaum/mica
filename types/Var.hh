/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MICA_VAR_HH
#define MICA_VAR_HH

#include <iostream>
#include <typeinfo>

#include <boost/cstdint.hpp>
#include <boost/operators.hpp>
#include <boost/cast.hpp>

#include "Data.hh"
#include "Ref.hh"
#include "Atoms.hh"
#include "OpCode.hh"
#include "Ref.hh"

#include "common/contract.h"

#ifdef DEBUG
#define ASSERT_D( x ) assert( x )
#else
#define ASSERT_D( x ) 
#endif
 
namespace mica
{
  /** Forward defined, but never used in core/.  Here so that 
   *  closures etc. can know their parent task.
   */
  class Data;
  class Symbol;

  /** Reference counting Smart pointer and dynamically-typed union.  
   *  Fits in 32-bits.
   */
  class Var
    : boost::totally_ordered<Var>,          // >, >=, <=, etc.
      boost::integer_arithmetic<Var>,       // + - / * %
      boost::bitwise<Var>,                  // & | ^
      boost::shiftable<Var>                 // << >>
  {
  private:

    /** Internal structures used for storage in the union
     */
    struct _Integer {
      bool            is_integer : 1;
      int                integer : 31;
    };

  public:

    /** Storage for floats
     */
    struct float_store
    {
      float           value;
      unsigned int    refcnt;

      static void free( float_store *ptr );
      void dncount() {
	refcnt--;
	if (!refcnt)
	  float_store::free( this );
      }
      void upcount() {
	refcnt++;
      }

    };

  public:

    /** Union/variant-type or reference to an object
     */
    union {
      uint32_t	      value;
      _Integer        integer;
      _Atom           atom;
    } v;

    
  public: 
    /** Returns the type identifier of what is held in the Var
     */
    inline Type::Identifier type_identifier() const {
      if (v.integer.is_integer) 
	return Type::INTEGER;
      else if (v.atom.is_float)
	return Type::FLOAT;
      else if (v.atom.is_pointer)
	return get_data()->type_identifier();
      else
	switch (v.atom.type) {
	case Atoms::CHAR:
	  return Type::CHAR;
	  break;
	case Atoms::OPCODE:
	  return Type::OPCODE;
	  break;
	case Atoms::BOOLEAN:
	  return Type::BOOL;
	  break;
	case Atoms::SYMBOL:
	  return Type::SYMBOL;
	  break;
	default:
	  ASSERT_D(0);
	}
      ASSERT_D(0);
      return Type::ABSTRACT;     // Shouldn't get here
    }

    template<typename R, typename T>
    R apply_visitor( const T &x ) const {
      if (v.integer.is_integer) 
        return x.operator()( boost::numeric_cast<int>(v.integer.integer) );
      else if (v.atom.is_float)
	return x.operator()( as_float() );
      else if (v.atom.is_pointer) {
	return x.operator()( get_data() );
      }  else
	switch (v.atom.type) {
	case Atoms::CHAR:
	  return x.operator()( (char)v.atom.value );
	  break;
	case Atoms::OPCODE:
	  return x.operator()( Op(v.atom) );
	  break;
	case Atoms::BOOLEAN:
	  return x.operator()( (bool)v.atom.value );
	  break;
	case Atoms::SYMBOL:
	  return x.operator()( as_symbol() );
	  break;
	default:
	  ASSERT_D(0);
	  break;
	}
      ASSERT_D(0);
      exit(-1);
    }

    template<typename R, typename T>
    R apply_dynamic_visitor( const T &x ) const {
      if (isData()) 
	return get_data()->apply_dynamic_visitor<R>( x );
      else
	return apply_visitor<R>( x );
    }

    
  public:
    bool invariant() const;

    /** construct a Var with its default value set to None
     */
    Var();

    /** construct a copy of a Var from another Var
     *  @param from what to copy from
     */
    Var( const Var &from );

    /** construct an INTEGER Var with initial value
     *  @param initial value of the Var
     */
    explicit Var( int initial );

    /** Convert from boolean.  False = None, true = integer 1
     *  @param initial initial value of the Var
     */
    explicit Var( bool initial );

    /** construct a DATA Var with initial value
     *  @param initial value of the Var
     */
    Var( Data *initial );

    /** construct a DATA Var with initial value
     *  @param initial value of the Var
     */
    Var( const Data *initial );

    /** convert a Ref to a Var
     *  @param initial value of the Var
     */
    template<class T>
    explicit Var( const Ref<T> &from ) { 
      v.value = 0; 
      set_data( dynamic_cast<Data*>( (T*)(from) ) );
    }

    /** construct a String Var from a char pointer
     *  @param initial string value of the Var
     */
    explicit Var( const char *initial );

    /** construct a char Var
     *  @param initial value of the Var
     */
    explicit Var( const char initial );

    /** construct a float Var
     *  @param float value of the float
     */
    explicit Var( const float initial );

    /** pass around a Symbol
     *  @param intial symbol object
     */
    explicit Var( const Symbol &symbol );

    /** construct an opcode Var
     *  @param initial value of the Var
     */
    explicit Var( const Op &initial );

    /** construct an opcode Var from just the operation
     *  @param initial value of the Var
     */
    explicit Var( const Op::Code &initial );

  public:
    /** Go out of scope -- decrease reference count
     */
    ~Var();

  public:
    /** Return the true value of this Var.
     *  which return the value they are currently pointing to.
     *  @return inner value of a Var
     */
    Var value() const;

    /** Return a clone of this object.  Returns an
     *  identical copy, except in case of Object, where
     *  an object child is created.
     *  @return the new clone
     */
    Var clone() const;
    
  public:
    /** Dereference Data item from Var
     */
    Data *operator->() const;

    /** Convert the low level repres
     */
    Symbol as_symbol() const;

  private:
    /** Return the float contents of the Var
     */
    float as_float() const;

    inline bool is_float() const {
      return !v.atom.is_integer && v.atom.is_float;
    }

    void set_float( float val );

  private:
    void set_data( Data * );

    Data *get_data() const;

    inline void upcount() {
      if (isData()) {
	get_data()->upcount();
      } else if (is_float())
	get_float()->upcount();
    }
    
    float_store *get_float() const;

    inline void dncount() {
      if (isData()) {
	get_data()->dncount();
      } else if (is_float()) {
	get_float()->dncount();
      }
    }

  public:
    /** These are the basic operators that boost::operators needs in 
     *  order to be able to construct the rest
     */

    /** equivalence comparison operator
     *  @param v2 right hand side of comparison
     *  @return truth value of comparison
     */
    bool operator==( const Var &v2 ) const;

    /** assignment operator
     *  @param rhs right hand side of the assignment
     *  @return a reference to "this", modified
     */
    Var& operator=(const Var& rhs);

    /** assignment operator for addition
     *  @param rhs right hand side of addition
     *  @return reference to self with right hand side added
     */
    Var& operator+=(const Var &arg2);

    /** assignment operator for subtraction
     *  @param rhs right hand side of subtraction
     *  @return reference to self with right hand side subtracted
     */
    Var& operator-=(const Var &arg2);

    /** assignment operator for multiplication
     *  @param rhs right hand side of multiplication
     *  @return reference to self multiplied by right hand side
     */
    Var& operator*=(const Var &arg2);

    /** assignment operator for division
     *  @param rhs right hand side of division
     *  @return reference to self divided by right hand side
     */
    Var& operator/=(const Var &arg2);

    /** assignment operator for integer modulus
     *  @param rhs right hand side of modulus
     *  @return reference to self mod rhs
     */
    Var& operator%=(const Var &arg2);

  public:
    /** bitwise shift left operator
     *  @param arg2 right hand side of operation
     *  @return result of or
     */
    Var& operator<<=( const Var &arg );

    /** bitwise shift left operator
     *  @param arg2 right hand side of operation
     *  @return result of or
     */
    Var& operator>>=( const Var &arg );

    /** bitwise and operator
     *  @param arg2 right hand side of operation
     *  @return result of or
     */
    Var& operator&=( const Var &arg );

    /** bitwise or operator
     *  @param arg2 right hand side of operation
     *  @return result of or
     */
    Var& operator|=( const Var &arg );

    /** exclusive or operator
     *  @param arg2 right hand side of operation
     *  @return result of exclusive or
     */
    Var& operator^=(const Var &arg);
   
    /** less-than comparison
     *  @param v2 right hand side of comparison
     *  @return truth value of comparison
     */
    bool operator<(const Var &v2) const;

  public:
    /** These are all conviences to make assigning from values
     ** easier.   Some would argue that having these be more explicit
     ** would be better for the world.
     */

    /** assignment to Data
     */
    Var& operator= (Data *rhs );

    /** assignment to int
     */
    Var& operator= (int from);

    /** assignment to string
     */
    Var& operator= (char *from);

    /** assignment to char
     */
    Var& operator= (char from);

    /** assignment to boolean
     */
    Var& operator= (bool from);

    /** assignment to opcode
     */
    Var& operator= ( const Op &op );

    Var& operator= ( const Symbol &sym );

    bool operator==( int b2 ) const;

    bool operator==( char b2 ) const;

    bool operator==( const Op &op ) const;

    bool operator==( const Symbol &sym ) const;


  public: 
    /** comparison
     *  @param value to compare with ( this comes first) 
     *  @return -1 if this < v2, 0 if equal, 1 if this > v2 
     */
    int compare(const Var &v2) const;

    /** return truth value of value
     */
    bool truth() const;

  public:
    /** logical and operator
     *  @param arg2 right hand side of operation
     *  @return result of and
     */
    Var operator&&( const Var &arg ) const;

    /** logical or operator
     *  @param arg2 right hand side of operation
     *  @return result of or
     */
    Var operator||( const Var &arg ) const;

    /** logical not operator
     */
    bool operator!() const;

  public:
    /** negate operator
     *  @return result of negation
     */
    Var operator-() const;

  public:

    /** is the object storing heap-allocated Data?
     */
    inline bool isData() const {
      return (!v.atom.is_integer && !v.atom.is_float) && v.atom.is_pointer;
    }

    /** is the type atomic?
     *  @return true if not aggregate
     */
    bool isAtom() const;

    /** is the type aggregate
     *  @return true if type can contain other types
     */
    bool isSequence() const;

    /** is the type numeric
     */
    bool isNumeric() const {
      return v.integer.is_integer;
    }

    /** is this a method?
     */
    bool isBlock() const;

    /** is this an object?
     */
    bool isObject() const;

  public:
    /** Return the list of delegates of this object
     */
    var_vector delegates() const;

  public:
    /** Apply this object as a function, with the given arguments
     */
    var_vector perform( const Ref<Frame> &caller, const Var &args );

  public:
    /** Extract a subsequence from a sequence.  
     *  @param start start index of the subsequence
     *  @param length length of the subsequence
     *  @return subsequence, or empty if start is negative, or entire
     *          remainder if length is too large
     */
    Var subseq( int start, int length ) const;

    /** @param i index to retrieve an element from
     *  @return the element at the given index.  
     *  @throw out_of_range if the index is out of bounds
     */
    Var lookup( const Var &i ) const;

    /** add a new element to the front/left of a sequence
     *  if value is not a sequence, create a sequence of
     *  the two values.
     *  @param N element to add
     *  @return sequence containing the new element at the front
     */
    Var cons( const Var &el ) const;

    /** @return return the first element of the sequence. (car)
     *  @throws invalid_type on non-sequences
     *  @throws out_of_range if the sequence is empty
     */
    Var lhead() const;
     
    /** @return sequence minus the first element.  (Cdr)
     *  returns empty() if the sequence is already empty
     *  @throws invalid_type on non-sequences
     */
    Var ltail() const;
    
    
  public:
    /** Return operations for application over a range
     */
    var_vector map( const Var &expr ) const ;

    /** Return all elements flattened
     */
    var_vector flatten() const;

  public:

    /** Various conversions
     */

    /** Attempt conversion to C integer
     */
    int toint() const;
    
    /** Attempt conversion to C 'char'
     */
    char tochar() const;

    /** Return a string conversion of the inside of the Var
     */
    mica_string tostring() const;

    /** Return a printable representation of the inside of the Var
     */
    mica_string rep() const;

    /** Append as a string to an ostream.  Used by operator<<(ostream).
     */
    std::ostream &append( std::ostream &lhs ) const;

  public:
    /** @return serialized form suitable for storage
     */
    void serialize_to( serialize_buffer &s_form ) const;

    /** Return all pointers to reference counted objects
     *  held inside the contents of this Var.
     */
    void append_child_pointers( child_set &child_list );

  public:
    /** Slot operations
     */
    Var declare( const Var &accessor, const Symbol &name,
		 const Var &value );

    OptSlot get( const Var &accessor, const Symbol &name ) const;

    Var assign( const Var &accessor, const Symbol &name, const Var &value );
    
    void remove( const Var &accessor, const Symbol &name );
    
    Var slots() const;
    
  public:
    /** Return a hashable value for the contents of this Var
     */
    unsigned int hash() const;

  } __attribute__ ((packed)); ;

  /** OptSlot results from slot get are a pair of definer, value
   */
  struct Slot {
    Var definer;
    Var value;

    Slot( const Var &idefiner, const Var &ivalue )
      : definer(idefiner), value(ivalue) {}

    bool operator==( const Slot &cmp ) const {
      return definer == cmp.definer && value == cmp.value;
    }

    Slot &operator=( const Slot &rhs ) {
      if (&rhs != this) {
	definer = rhs.definer;
	value = rhs.value;
      }
      return *this;
    }
  };

  /** Easy reference to a #none symbol.  
   *  Definition in GlobalSymbols.cc
   *  Created by initSymbols()
   */
  extern Var NONE;

  std::ostream& operator << (std::ostream &, const mica::Var &);

  /** For serializing a var vector
   */
  inline void SerializeVV( mica::serialize_buffer &S, const var_vector &vv ) {
    Pack( S, vv.size() );
    for (var_vector::const_iterator vi = vv.begin(); vi != vv.end(); vi++) {
      vi->serialize_to( S );
    }
  }

}



#endif /* VAR_HH */

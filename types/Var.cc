/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"
#include "common/contract.h"

#include <cassert>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <signal.h>
#include <setjmp.h>
#include <malloc.h>

#define BOOST_NO_LIMITS_COMPILE_TIME_CONSTANTS
#include <boost/type_traits/is_const.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/pool/object_pool.hpp>

#include "OpCode.hh"
#include "Data.hh"

#include "Var.hh"
#include "String.hh"
#include "Exceptions.hh"
#include "Error.hh"
#include "GlobalSymbols.hh"
#include "MetaObjects.hh"
#include "List.hh"
#include "Symbol.hh"

#include "logging.hh"

using namespace mica;
using namespace std;

#define CLEAR_BITS(x,mask) (( (x) & ~(mask)))
#define TO_POINTER(x) CLEAR_BITS(x,4)
#define TO_FLOAT_POINTER(x) CLEAR_BITS(x,3)

static boost::object_pool<Var::float_store> float_pool;
void Var::float_store::free( Var::float_store *ptr ) {
  float_pool.destroy( ptr );
}

/** For divzero protection
 */
jmp_buf env;

void 
signal_handler (int sig) 
{
  assert( sig == SIGFPE );
  longjmp(env, sig); 
}

Symbol Var::as_symbol() const {
  return Symbol(v.atom);
}



/** Get child pointers from what's in a Var
 */
struct child_pointers_visitor {
  template<typename T>
  inline child_set operator()( const T&  ) const {
    return child_set();
  }   
  inline child_set operator()( Data *t ) const {
    return t->child_pointers();
  }
};

/** Get the delegates for what's in Var.
 */
struct delegates_visitor {
  inline var_vector operator()(const int &) const { 
    return MetaObjects::delegates_for( Type::INTEGER );
  }
  inline var_vector operator()(const float &) const { 
    return MetaObjects::delegates_for( Type::FLOAT );
  }
  inline var_vector operator()(const char &) const { 
    return MetaObjects::delegates_for( Type::CHAR );
  }
  inline var_vector operator()(const bool &) const { 
    return MetaObjects::delegates_for( Type::BOOL );
  }
  inline var_vector operator()(const Op &) const { 
    return MetaObjects::delegates_for( Type::OPCODE );
  }
  inline var_vector operator()(const Symbol &) const { 
    return MetaObjects::delegates_for( Type::SYMBOL );
  }
  inline var_vector operator()(Data *t) const { 
    return t->delegates(); 
  }
    
  template<typename T>
  inline var_vector operator()(const T& t) const
  {
    assert(0);
  }
};

/** Visitor that evaluates the truth of a Var
 */
struct truth_visitor {

  template<typename T>
  inline bool operator()( const T& x  ) const {
    return (bool)x;
  }
  
  inline bool operator()( const Op &op ) const {
    return false;
  }

  inline bool operator()( const Symbol &sym ) const {
    return true;
  }

  inline bool operator()( Data *t ) const {
    return t->truth();
  }
};

/** Visitor that evaluates the negation of Var contents
 */
struct neg_visitor {

  template<typename T>
  inline Var operator()( const T& x  ) const {
    return Var( - x );
  }

  inline Var operator()( const Op &op ) const {
    throw invalid_type("cannot negate opcode");
  }

  inline Var operator()( const Symbol &sym ) const {
    throw invalid_type("cannot negate opcode");
  }

  inline Var operator()( Data *x ) const {
    return Var( x->neg() );
  }
};

/** Visitor to obtain a hash from the contents of a Var
 */
struct hashing_visitor { 

  inline unsigned int operator()( int y ) const {
    return STD_EXT_NS::hash<int>()(y);
  }
  inline unsigned int operator()( char y ) const {
    return STD_EXT_NS::hash<char>()(y);
  }
  inline unsigned int operator()( bool y ) const {
    return y ? 0 : 1;
  }
  inline unsigned int operator()( float y ) const {
    return 
      STD_EXT_NS::hash<unsigned long>()(boost::numeric_cast<unsigned long>(y));
  }
  inline unsigned int operator()( const Op &y ) const {
    return STD_EXT_NS::hash<int>()( (int)((Var)y).v.value );
  }
  inline unsigned int operator()( const Symbol &y ) const {
    return y.hash();
  }
  inline unsigned int operator()( Data *x ) const {
    return x->hash();
  }

  template< typename X >
  inline unsigned int operator()( int y ) const {
    // DEFAULT
    assert(0);
  }
};

/** Visitor to obtain a serialization of the contents of a Var
 */
struct serializing_visitor { 

  template<typename X>
  inline mica_string operator()( const X &y ) const {
    mica_string x;
    Pack( x, y );
    return x;
  }

  inline mica_string operator()( const Symbol &y ) const {
    return y.serialize();
  }

  inline mica_string operator()( Data *x ) const {
    return x->serialize();
  }
};

/** Return a string conversion of the held item
 */
struct tostring_visitor { 
  template< typename X >
  inline mica_string operator()( const X &y ) const {
    std::ostringstream dstr;
    dstr << y;
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    return mica_string(dstr.str().c_str());
  }
  inline mica_string operator()( const Symbol &y ) const {
    return y.tostring();
  }
  inline mica_string operator()( const Op &op ) const {
    return operator()( op.code );
  }
  inline mica_string operator()( Data *x ) const {
    return x->tostring();
  }
};

struct rep_visitor { 

  inline mica_string operator()( const Op &op ) const {
    std::ostringstream dstr;
    dstr << 'O' << op.code;
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    return mica_string(dstr.str().c_str());
  }
  inline mica_string operator()( const Symbol &sym ) const {
    std::ostringstream dstr;
    dstr << '#' << sym.tostring();
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    return mica_string(dstr.str().c_str());
  }

  inline mica_string operator()( const char &ch ) const {
    std::ostringstream dstr;
    dstr << '\'' << ch << '\'';
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    return mica_string(dstr.str().c_str());
  }

  inline mica_string operator()( const bool &bl ) const {
    std::ostringstream dstr;
    if (bl)
      dstr << "true";
    else
      dstr << "false";
    
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    return mica_string(dstr.str().c_str());
  }

  template< typename X >
  inline mica_string operator()( const X &y ) const {
    std::ostringstream dstr;
    dstr << y;
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    return mica_string(dstr.str().c_str());
  }

  inline mica_string operator()( Data *x ) const {
    return x->rep();
  }
};

/** Visitor to perform a flatten
 */
struct flatten_visitor {
  template<typename T>
  inline var_vector operator()( const T&x  ) const {
    var_vector ops;
    ops.push_back( Var(x) );
    return ops;
  }
    
  inline var_vector operator()( Data *t ) const {
    return t->flatten();
  }
};

static child_pointers_visitor child_pointers_v;
static delegates_visitor delegate_v;
static truth_visitor truth_v;
static neg_visitor neg_v;
static hashing_visitor hasher;
static serializing_visitor serializer;
static tostring_visitor tostring_v;
static rep_visitor rep_v;
static flatten_visitor flatten_v;


Data *Var::operator->() const {
  PRECONDITION(isData());
  return get_data();
};
    
// normally this would be after the constructors 
// but there's just SO MANY OF THEM
bool Var::invariant() const
{
  // this is also Data's invariant, but it's quite possible that Var
  // could be holding a bad ref, and if the invariant ever becomes
  // virtual, then this invariant itself would have erratic
  // behavior.  The guard check isn't much good, but it's better
  // than nothing.
  if (isData()) 
    return get_data();

  // checking refcnt here was wrong because odd things can happen to the
  // reference counts when the cyclic detector is playing with them.
  //   	&& v.data->refcnt >= 1;
  else
    return true;
} 

void Var::set_data( Data *data ) {
  /** Avoid toggling the refcount -- cost of a comparison, but worth it
   */
  if (isData() && (data == get_data()))
    return;

  dncount();

  v.value = reinterpret_cast<uint32_t>(data);
  
  v.atom.is_integer = false;
  v.atom.is_float   = false;
  v.atom.is_pointer = true;

  assert( get_data() == data );

  upcount();
}

Data *Var::get_data() const {
  PRECONDITION(isData());
  return reinterpret_cast<Data*>( TO_POINTER(v.value) );
}

float Var::as_float() const {
  return get_float()->value;
}

Var::float_store* Var::get_float() const {
  return reinterpret_cast<float_store*>( TO_FLOAT_POINTER(v.value) );
}

void Var::set_float( float val ) {
  dncount();

  float_store *float_val = float_pool.construct();
  
  float_val->refcnt = 1;
  float_val->value = val;

  v.value = reinterpret_cast<uint32_t>(float_val);
  v.atom.is_integer = false;
  v.atom.is_float   = true;

  assert( as_float() == val );
}


Var::~Var() {
  dncount();
}

// Default constructor -- return NONE instance
Var::Var()
{
  v.value = NONE.v.value;
  assert(!v.atom.is_float);
  assert(!v.atom.is_pointer);
}

// Copy constructor
Var::Var( const Var &from )
  : v(from.v)
{
  upcount();

  // This is a constructor, so we can't check our own invariant, and
  // thus don't use a normal precondition.  The invariant for what
  // we're copying however, can and does hold.
  ASSERT(from.invariant());
}

Var::Var( int initial )
{ 
  v.integer.is_integer = true;
  v.integer.integer = initial;
}

Var::Var( bool initial )
{
  v.atom.is_integer = false;
  v.atom.is_float   = false;
  v.atom.is_pointer = false;
  v.atom.type = Atoms::BOOLEAN;
  v.atom.value = initial;
}

Var::Var( char initial )
{ 
  v.atom.is_integer = false;
  v.atom.is_float   = false;
  v.atom.is_pointer = false;
  v.atom.type = Atoms::CHAR;
  v.atom.value = initial;
}

Var::Var( float initial )
{ 
  set_float(initial);
}

Var::Var( const Symbol &sym )
{
  memcpy( this, &sym, sizeof(sym) );
}

Var::Var( const Op &op )
{
  memcpy( this, &op, sizeof(op) );
}


Var::Var( const Op::Code &code )
{
  Op op( code );
  memcpy( this, &op, sizeof(op));
}

Var::Var( const char *from )
{
  v.value = 0;
  operator=(String::from_cstr(from));
}

Var::Var( Data *initial )
{
  v.value = 0;
  // constructor, can't use PRECONDITION
  ASSERT(Data::static_invariant(initial));
  set_data( initial );
}

Var::Var( const Data *initial )
{
  v.value = 0;
  // constructor, can't use PRECONDITION
  ASSERT(Data::static_invariant(initial));
  set_data( const_cast<Data*>(initial) );
}


Var Var::clone() const
{
  if (isData())
    return get_data()->clone();
  else
    return *this;
}

Var Var::value() const
{
  if (isData())
    return get_data()->clone();
  else
    return *this; 
}

// Assignment operator
Var &Var::operator=( const Var &from )
{
  if (this != &from) {

    dncount();
  
    // copy it over
    v = from.v;
    
    upcount();
  }

  return *this;
}

Var &Var::operator=( Data *rhs )
{
  set_data( rhs );
  return *this;
}

Var &Var::operator=( int from ) {
  dncount();

  v.integer.is_integer = true;
  v.integer.integer = from;

  return *this;
}

Var &Var::operator=( char *from ) {
  dncount();

  operator=(String::from_cstr(from));

  return *this;
}

Var &Var::operator=( char from ) {
  dncount();

  v.atom.is_integer = false;
  v.atom.is_float   = false;
  v.atom.is_pointer = false;
  v.atom.type = Atoms::CHAR;
  v.atom.value = from;

  return *this;
}

Var &Var::operator=( bool from ) {
  dncount();

  v.atom.is_integer = false;
  v.atom.is_float   = false;
  v.atom.is_pointer = false;
  v.atom.type = Atoms::BOOLEAN;
  v.atom.value = from;

  return *this;
}

Var &Var::operator=( const Op &op ) {
  dncount();

  memcpy( this, &op, sizeof(op) );
  return *this;
}

Var &Var::operator=( const Symbol &sym ) {
  dncount();

  memcpy( this, &sym, sizeof(sym) );
  return *this;
}

#define COMPARE_OPERATION( NAME, OP ) \
template<typename T> struct NAME { \
  const T &value; \
  explicit NAME ( const T &i_value )  \
    : value(i_value) {} \
  template<typename X> \
  bool operator()( const X & ) const { \
    return false; \
  } \
  bool operator()( const T &x ) const { \
    return value OP x; \
  } \
}; \
struct NAME <Data*> { \
  const Data *value; \
  explicit NAME ( const Data *i_value ) \
    : value(i_value) {} \
  template<typename X> \
  bool operator()( const X &x ) const { \
      return (*value) OP ( Var(x) ); \
  } \
}; \
struct NAME ##_visitor { \
  const Var &lhs; \
  explicit NAME ##_visitor( const Var &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename T> \
  bool operator()( const T &x ) const {  \
    return lhs.template apply_visitor<bool>( NAME <T>(x) ); \
  } \
};

COMPARE_OPERATION( equality, == );
COMPARE_OPERATION( less_than, < );

bool Var::operator==( const Var &v2 ) const
{
  if (&v2 == this)
    return true;
  
  return apply_visitor<bool>( equality_visitor( v2 ) );
}

bool Var::operator==( int v2 ) const
{
  return apply_visitor<bool>( equality<int>(v2) );
}

bool Var::operator==( char v2 ) const
{
  return apply_visitor<bool>( equality<char>(v2) );
}

bool Var::operator==( const Op &op ) const
{
  return apply_visitor<bool>( equality<Op>(op) );
}

bool Var::operator==( const Symbol &sym ) const
{
  return apply_visitor<bool>( equality<Symbol>(sym) );
}

bool Var::operator<(const Var &v2) const
{
  return apply_visitor<bool>( less_than_visitor( v2 ) );
}

Var Var::operator&&( const Var &rhs ) const
{
  /** If left is true, return right.  Otherwise return left.
   */
  if (truth())
    return rhs;
  else
    return *this;
}

Var Var::operator||( const Var &rhs ) const
{
  /** If left hand side is true, return it.  Otherwise return 
   *  right hand side.
   */
  if (truth())
    return *this;
  else
    return rhs;
}


bool Var::truth() const
{
  return apply_visitor<bool>( truth_v );
}

bool Var::operator!() const
{
  return !truth();
}

/** This macro builds visitors to perform arithmetic operations.
 *  These visitors and the ones for bitwise operations (see below)
 *  should maybe factored out into another file, for readability
 */
#define ARITHMETIC_OPERATION( NAME, OP ) \
template<typename T> struct NAME ##_op { \
  const T &lhs; \
  explicit NAME ##_op( const T &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    return Var(lhs OP rhs); \
  } \
  inline Var operator()( const float &rhs ) const { \
    return Var(boost::numeric_cast<float>(lhs) OP rhs); \
  } \
  inline Var operator()( const Op &op ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( const Symbol &op ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( Data *rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_op<Data *> { \
  const Data *lhs; \
  explicit NAME ##_op( Data *i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    return lhs-> NAME (Var(rhs)); \
  } \
}; \
struct NAME ##_op<float> { \
  const float &lhs; \
  explicit NAME ##_op( const float &i_lhs ) \
    : lhs(i_lhs) {}; \
  inline Var operator()( const bool &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( const Symbol &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( const Op &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( Data *rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    return Var(lhs OP boost::numeric_cast<float>(rhs)); \
  } \
}; \
struct NAME ##_op<Op> { \
  const Op &lhs; \
  explicit NAME ##_op( const Op &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_op<Symbol> { \
  const Symbol &lhs; \
  explicit NAME ##_op( const Symbol &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_visitor { \
  const Var &rhs; \
  explicit NAME ##_visitor( const Var &i_rhs ) \
    : rhs(i_rhs) {}; \
  template<typename T> \
  inline Var operator()( const T &lhs ) const { \
    return rhs.template apply_visitor<Var>( NAME ##_op<T>(lhs) ); \
  } \
  inline Var operator()( const bool &lhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
};

/** Construct the four basic arithmetic operators using the above
 *  macro
 */
ARITHMETIC_OPERATION( add, + );
ARITHMETIC_OPERATION( sub, - );
ARITHMETIC_OPERATION( div, / );
ARITHMETIC_OPERATION( mul, * );

Var &Var::operator+=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( add_visitor(rhs) ) );
}
Var &Var::operator*=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( mul_visitor(rhs) ) );
}
Var &Var::operator-=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( sub_visitor(rhs) ) );
}
Var &Var::operator/=( const Var &rhs ) {
  signal (SIGFPE, signal_handler);

   if (setjmp(env) == 0) 
     operator=( apply_visitor<Var>( div_visitor(rhs) ) );
   else {
     throw divzero_error("division by zero");
   }
   return *this;
}

/** Divmod visitors -- same as the macro mostly, except that we 
 *  have to make a special case for floats (they use %).  Weakness
 *  of the above macro that it can't really vary that much.
 */
template<typename T> struct mod_op { 
  const T &lhs; 
  explicit mod_op( const T &i_lhs ) 
    : lhs(i_lhs) {}; 
  template<typename LHT>
  inline Var operator()( const LHT &rhs ) const { 
    throw invalid_type("invalid operands"); 
  } 
}; 
struct mod_op<Data *> { 
  const Data *lhs; 
  explicit mod_op( Data *i_lhs ) 
    : lhs(i_lhs) {}; 
  template<typename LHT> 
  inline Var operator()( const LHT &rhs ) const { 
    return lhs->mod(Var(rhs)); 
  } 
}; 
struct mod_op<int> { 
  int lhs; 
  explicit mod_op( int i_lhs ) 
    : lhs(i_lhs) {}; 
  template<typename LHT>
  inline Var operator()( const LHT &rhs ) const { 
    throw invalid_type("invalid operands"); 
  } 
  inline Var operator()( const int &rhs ) const { 
    return Var( lhs % rhs );
  } 
}; 
struct mod_op<float> { 
  float lhs; 
  explicit mod_op( float i_lhs ) 
    : lhs(i_lhs) {}; 
  template<typename LHT>
  inline Var operator()( const LHT &rhs ) const { 
    return Var( fmod( lhs, boost::numeric_cast<float>(rhs) ) );
  } 
  inline Var operator()( const bool &rhs ) const { 
    throw invalid_type("invalid operands"); 
  } 
  inline Var operator()( Data *rhs ) const { 
    throw invalid_type("invalid operands"); 
  } 
  inline Var operator()( const Symbol &rhs ) const { 
    throw invalid_type("invalid operands"); 
  } 
  inline Var operator()( const Op &rhs ) const { 
    throw invalid_type("invalid operands"); 
  } 
}; 
struct mod_visitor  { 
  const Var &rhs; 
  explicit mod_visitor( const Var &i_rhs ) 
    : rhs(i_rhs) {}; 
  template<typename T> 
  inline Var operator()( const T &lhs ) const { 
    return rhs.template apply_visitor<Var>( mod_op<T>(lhs) ); 
  } 
};

Var &Var::operator%=( const Var &rhs ) {

  signal (SIGFPE, signal_handler);

  if (setjmp(env) == 0) 
    operator=( apply_visitor<Var>( mod_visitor(rhs) ) );
  else {
    throw divzero_error("division by zero in divmod");
  }
  return *this;

}


/** This macro implements a visitor for a bitwise operation
 */
#define BITWISE_OPERATION( NAME, OP ) \
template<typename T> struct NAME ##_op  { \
  const T &lhs; \
  explicit NAME ##_op( const T &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    return Var(lhs OP rhs); \
  } \
  inline Var operator()( const float &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( const Op &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( const Symbol &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
  inline Var operator()( Data *rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_op<Data *> { \
  const Data *lhs; \
  explicit NAME ##_op( const Data *i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    return lhs-> NAME (Var(rhs)); \
  } \
}; \
struct NAME ##_op<float> { \
  const float &lhs; \
  explicit NAME ##_op( const float &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_op<Op> { \
  const Op &lhs; \
  explicit NAME ##_op( const Op &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_op<Symbol> { \
  const Symbol &lhs; \
  explicit NAME ##_op( const Symbol &i_lhs ) \
    : lhs(i_lhs) {}; \
  template<typename LHT> \
  inline Var operator()( const LHT &rhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
}; \
struct NAME ##_visitor { \
  const Var &rhs; \
  explicit NAME ##_visitor( const Var &i_rhs ) \
    : rhs(i_rhs) {}; \
  template<typename T> \
  inline Var operator()( const T &lhs ) const { \
    return rhs.template apply_visitor<Var>( NAME ##_op<T>(lhs) ); \
  } \
  inline Var operator()( const bool &lhs ) const { \
    throw invalid_type("invalid operands"); \
  } \
};


BITWISE_OPERATION( eor, ^ );
BITWISE_OPERATION( band, & );
BITWISE_OPERATION( bor, | );
BITWISE_OPERATION( lshift, << );
BITWISE_OPERATION( rshift, >> );

Var &Var::operator|=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( bor_visitor(rhs) ) );
}
Var &Var::operator&=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( band_visitor(rhs) ) );
}
Var &Var::operator^=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( eor_visitor(rhs) ) );
}
Var &Var::operator<<=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( lshift_visitor(rhs) ) );
}
Var &Var::operator>>=( const Var &rhs ) {
  return operator=( apply_visitor<Var>( rshift_visitor(rhs) ) );
}

Var Var::operator-() const
{
  return apply_visitor<Var>( neg_v );
}

bool Var::isAtom() const
{
  if (isData())
    return get_data()->isAtom();
  else
    return true;
}

bool Var::isSequence() const
{
  return !isAtom();
}

bool Var::isObject() const
{
  if (isData())
    return get_data()->isObject();
  else
    return false;
}

bool Var::isBlock() const
{
  if (isData())
    return get_data()->isBlock();
  else
    return false;
}

var_vector Var::delegates() const {
  return apply_visitor<var_vector>( delegate_v );
}

template<typename T>
struct conversion {

  inline T operator()( const T& x ) const {
    return x;
  }

  template<typename X>  
  inline T operator()( const X & ) const {
    throw invalid_type("cannot convert value");
  }
};
struct conversion<int> {
  
  inline int operator()( const int &x ) const {
    return x;
  }

  inline int operator()( const float& x ) const {
    return boost::numeric_cast<int>(x);
  }

  inline int operator()( const char& x ) const {
    return boost::numeric_cast<int>(x);
  }

  inline int operator()( const bool& x ) const {
    return x ? 0 : 1;
  }

  inline int operator()( const Op& x ) const {
    throw invalid_type("cannot convert opcode to integer");
  }

  inline int operator()( Data *x ) const {
    return x->toint();
  }

  template<typename X>  
  inline int operator()( const X & ) const {
    throw invalid_type("cannot convert value to integer");
  }
};

struct conversion<float> {

  inline float operator()( const float &x ) const {
    return x;
  }

  inline float operator()( const int& x ) const {
    return boost::numeric_cast<float>(x);
  }

  inline float operator()( const char& x ) const {
    return boost::numeric_cast<float>(x);
  }

  inline float operator()( const bool& x ) const {
    return x ? 0 : 1;
  }

  template<typename X>  
  inline float operator()( const X & ) const {
    throw invalid_type("cannot convert value to float");
  }

};



int Var::toint() const
{
  return apply_visitor<int>( conversion<int>() );
}

char Var::tochar() const
{
  return apply_visitor<char>( conversion<char>() );
}


std::ostream &Var::append( std::ostream &lhs ) const
{
  lhs << rep();
  return lhs;
}


mica_string Var::tostring() const
{
  return apply_visitor<mica_string>( tostring_v );
}

mica_string Var::rep() const
{
  return apply_visitor<mica_string>( rep_v );
}

mica_string Var::serialize() const
{
  mica_string s;
  
  /** Push the type.
   */
  Pack( s, type_identifier() );
  
  s.append( apply_visitor<mica_string>( serializer ) );
  
  return s;
  
}

Var Var::subseq( int start, int length ) const {
  if (!isData())
    throw invalid_type("attempt to extract subseq from scalar operand");
  else
  return get_data()->subseq( start, length );
}

Var Var::lookup( const Var &index ) const {
  if (!isData())
    throw invalid_type("attempt to lookup item inside scalar operand");

  return get_data()->lookup( index );
}

Var Var::cons( const Var &el ) const {
  if (!isData())
    return List::tuple( *this, el );
  else
    return get_data()->cons( el );
}

Var Var::ltail() const {
  if (!isData())
    throw invalid_type("ltail on invalid type");
  else
    return get_data()->ltail();
}

Var Var::lhead() const {
  if (!isData())
    throw invalid_type("lhead on invalid type");
  else
    return get_data()->lhead();
}

var_vector Var::map( const Var &expr ) const
{
  if (!isData())
    throw invalid_type("attempt to iterate on non-sequence type");

  return get_data()->map( expr );
}

var_vector Var::flatten() const
{
  return apply_visitor<var_vector>( flatten_v );
}

 
var_vector Var::perform( const Ref<Frame> &caller, const Var &args )
{
  if (!isData())
    throw unimplemented("perform unimplemented on scalar type");

  return get_data()->perform( caller, args );
}


Var Var::declare( const Var &accessor, const Symbol &name, 
		  const Var &value) 
{
  if (isData())
    return get_data()->declare( accessor, name, value );
  else 
    return MetaObjects::AtomMeta.declare( accessor, name, value );
  
}

OptSlot Var::get( const Var &accessor, const Symbol &name ) const
{
  if (isData())
    return get_data()->get( accessor, name );
  else 
    return OptSlot();
  
}

Var Var::assign( const Var &accessor, const Symbol &name, 
		 const Var &value ) {
  if (isData())
    return get_data()->assign( accessor, name, value );
  else
    throw E_PERM;
}

void Var::remove(const Var &accessor, const Symbol &name ) {
  if (isData())
    get_data()->remove( accessor, name );
  else
    throw E_PERM;
}

Var Var::slots() const {
  if (isData())
    return get_data()->slots();
  else
    return List::empty();
}

child_set Var::child_pointers() {
  return apply_visitor<child_set>( child_pointers_v );
}


unsigned int Var::hash() const {
  return apply_visitor<unsigned int>( hasher );
}

std::ostream &mica::operator << (std::ostream &lhs, const Var &rhs)
{
  return rhs.append(lhs);
}


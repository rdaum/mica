/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <sstream>
#include <algorithm>
#include <utility>
#include <boost/cast.hpp>

#include "Data.hh"
#include "Var.hh"
#include "Exceptions.hh"
#include "List.hh"

#include "MetaObjects.hh"

using namespace mica;

/** These are private
 */
List::List()
  : Data(), var_vector()
{
}

List::List( const var_vector &from )
  : Data(), var_vector(from)
{
}

/** Everything below here is public
 */
Var List::from_vector( const var_vector &from )
{
  if (from.empty())
    return List::empty();
  else {
    return new (aligned) List(from);
  } 
}

var_vector List::as_vector() const {
  return *this;
}

Var List::single( const Var &el ) {
  var_vector s;
  s.push_back( el );
  return new (aligned) List(s);
}

Var List::tuple( const Var &left, const Var &right ) {
  var_vector s;
  s.push_back( left );
  s.push_back( right );
  return new (aligned) List(s);
}

Var List::triple( const Var &one, const Var &two, const Var &three ) {
  var_vector s;
  s.push_back( one );
  s.push_back( two );
  s.push_back( three );
  return new (aligned) List(s);
}

bool List::operator==( const Var &rhs ) const
{
  if (rhs.type_identifier() != type_identifier())
    return false;

  List *other = rhs->asType<List*>();
  
  return this == other || *this == *other;
}

bool List::operator<( const Var &rhs ) const
{
  if (rhs.type_identifier() != type_identifier())
    return false;

  return *this < *(rhs->asType<List*>());
}

Var List::add( const Var &v2 ) const
{
  //  var_vector x = *this + v2;
  var_vector x(*this);
  x.push_back( v2 );

  return new (aligned) List(x);
}

Var List::sub( const Var &v2 ) const
{
  throw unimplemented("sublist subtraction");
}

Var List::mul( const Var &v2 ) const
{
  /** Multiplying empty lists is stupid
   */
  if (this->var_vector::empty())
    return List::empty();

  /** Make X copies of this.
   */
  int copies = v2.toint();

  var_vector k;
  
  while (copies--) {
    //    k = k + *this;
    k.insert( k.end(), begin(), end() );
  }

  return new (aligned) List( k );
}

Var List::div( const Var &v2 ) const
{
  throw unimplemented("invalid operand for division");
}


Var List::cons( const Var &el ) const {
  var_vector n_vec;
  n_vec.push_back( el );
  n_vec.insert( n_vec.end(), begin(), end() );

  return List::from_vector( n_vec );
}


Var List::snoc( const Var &el ) const {
  return this->add( el );
}

Var List::append( const Var &seq ) const {
  var_vector result(*this);
  var_vector to_append(seq.flatten());
  result.insert( result.end(), to_append.begin(), to_append.end() );

  return List::from_vector( result );
}

Var List::lview() const {
  if (null())
    return List::empty();

  var_vector viewr( size() > 1 ? (begin() + 1) : begin(), 
		    end() );

  return List::tuple( List::single( front() ), 
		      List::from_vector(viewr) );
}

Var List::lhead() const {
  if (null())
    throw out_of_range("no head for empty sequence");

  return (*begin());
}

Var List::ltail() const {
  if (null())
    return empty();

  var_vector res(begin() + 1, end());
  return List::from_vector(res);
}


Var List::rview() const {
  if (null())
    return empty();

  var_vector viewr( begin(), 
		    size() > 1 ? (end() - 1) : end() );
    
  return List::tuple( List::single( back() ),
		      List::from_vector(viewr) );
}

Var List::rhead() const {
  if (null())
    throw out_of_range("no head for empty sequence");

  return back();
}

Var List::rtail() const {
  if (null())
    return empty();

  var_vector res(*this);
  res.pop_back();
  return List::from_vector(res);
}

bool List::null() const {
  return this->var_vector::empty();
}

int List::size() const {
  return boost::numeric_cast<int>(this->var_vector::size());
}

Var List::concat() const {
  var_vector result;
  for (var_vector::const_iterator x = begin(); x != end(); x++) {
    var_vector flattened( x->flatten() );
    result.insert( result.end(), flattened.begin(), flattened.end() );
  }

  return List::from_vector( result );
}

Var List::reverse() const {
  if (this->var_vector::empty())
    return List::empty();

  var_vector result(*this);
  std::reverse( result.begin(), result.end() );
  return List::from_vector( result );
}

Var List::take( int i ) const {
  if (i > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (i < 0)
    return List::empty();
  else
    return List::from_vector( var_vector( begin(), begin() + i ) );
}

Var List::drop( int i ) const {
  if (i > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (i < 0)
    return List::empty();
  else
    return List::from_vector( var_vector( begin() + i, end() ) );
}

Var List::splitAt( int i ) const {
  if (i > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (i < 0)
    return List::empty();
  else {
    var_vector splitl( begin(), begin() + i );
    var_vector splitr( begin() + i, end() );

    return List::tuple( List::from_vector(splitl), 
			List::from_vector(splitr) );
  }
}

Var List::subseq( int start, int length ) const {
  if (start + length > boost::numeric_cast<int>(size()))
    return this;
  else if (start < 0)
    return List::empty();
  else if (length < 0)
    return List::from_vector( var_vector( begin() + start,
					  end() ) );
  else
    return List::from_vector( var_vector( begin() + start,
					  begin() + start + length ) );
}

bool List::inBounds( int i ) const {
  return (i < size());
}

Var List::lookup( const Var &N ) const {
  int i(N.toint());

  if (!inBounds(i))
    throw out_of_range("index out of range");

  return this->at(i);
}

Var List::lookupM( int i ) const {
  if (!inBounds(i))
    return NONE;
  else
    return this->at(i);
}


Var List::lookup_withDefault( int i, const Var &d ) const {
  if (!inBounds(i))
    return d;
  else
    return this->at(i);
}

Var List::update( int i, const Var &e ) const {
  if (!inBounds(i))
    return Var(this);
  else {
    var_vector result = *this;
    result[i] = e;
    return List::from_vector(result);
  }
}


Var List::zip( const Var &with ) const {

  var_vector result;
  var_vector right(with.flatten());

  var_vector::const_iterator left_i = begin();
  var_vector::const_iterator right_i = right.begin();
  while (left_i != end() && right_i != right.end()) {
    result.push_back( List::tuple( *left_i, *right_i ) );
    left_i++;
    right_i++;
  }

  return List::from_vector(result);
}

Var List::zipTriple( const Var &two, const Var &three ) const {

  var_vector result;
  var_vector two_v(two.flatten());
  var_vector three_v(three.flatten());

  var_vector::const_iterator one_i = begin();
  var_vector::const_iterator two_i = two_v.begin();
  var_vector::const_iterator three_i = three_v.begin();
  while (one_i != end() && two_i != two_v.end() &&
	 three_i != three_v.end()) {
    result.push_back( List::triple( *one_i, *two_i, *three_i ) );
    one_i++;
    two_i++;
    three_i++;
  }

  return List::from_vector(result);
}

Var List::unzip() const {
    
  var_vector one;
  var_vector two;

  for (var_vector::const_iterator z = begin(); z != end(); z++) {
    var_vector pair = z->flatten();
    if (pair.size() != 2)
      throw out_of_range("element is not a pair during unzip");
    one.push_back( pair[0] );
    two.push_back( pair[1] );
  }
  return List::tuple( List::from_vector( one ),
		      List::from_vector( two ) );
}

Var List::unzipTriple() const {
    
  var_vector one;
  var_vector two;
  var_vector three;

  for (var_vector::const_iterator z = begin(); z != end(); z++) {
    var_vector triple = z->flatten();
    if (triple.size() != 3)
      throw out_of_range("element is not a triple during unzipTriple");
    one.push_back( triple[0] );
    two.push_back( triple[1] );
    three.push_back( triple[2] );
  }
  return List::triple( List::from_vector( one ),
		       List::from_vector( two ),
		       List::from_vector( three ) );

}


var_vector List::flatten() const 
{
  var_vector ops( *this );
  return ops;
}

var_vector List::map( const Var &expr ) const
{
  /** Finished iterating.  No-op
   */
  if (null())
    return var_vector();

  /** Push cdr then push the rest of the expr
   */
  var_vector ops;
  ops.push_back( Var(Op::EVAL) );
  ops.push_back( expr );
  ops.push_back( lhead() ); // cdr

  /** recurse on car
   */
  if (size() > 1) {

    ops.push_back( Var(Op::MAP) );

    ops.push_back( expr );

    /** car
     */
    ops.push_back( ltail() );


  }
  
  return ops;
}


mica_string List::rep() const
{
  mica_string output = "[";
  var_vector::const_iterator si;
  for (si = begin(); si != end();) {
    output.append( (*si).rep() );
    si++;
    if (si == end())
      break;
    else
      output.append( ", " );
  }
  output.push_back(']');

  return output;
}


void List::serialize_to( serialize_buffer &s_form ) const {
  Pack( s_form, type_identifier() );

  /** Append list size
   */
  size_t len = size();

  Pack(s_form, len);

  /** Now append each element.
   */
  var_vector::const_iterator x;
  for (x = this->begin(); x != this->end(); x++)
    x->serialize_to( s_form );

}

size_t List::hash() const
{
  size_t start = 0;

  var_vector::const_iterator x;
  for (x = begin(); x != end(); x++)
    start += (*x).hash();

  return start;
}

void List::append_child_pointers( child_set &child_list ) {
  append_datas( child_list, flatten() );
}

int List::toint() const
{
  throw invalid_type("toint() called on collection");
}

float List::tofloat() const
{
  throw invalid_type("tofloat() called on collection");
}

Var List::mod( const Var &v2 ) const
{
  throw invalid_type("invalid operands");
}

Var List::neg() const
{
  throw invalid_type("invalid operand");
}

mica_string List::tostring() const
{
  throw invalid_type("cannot convert collection to string");
}


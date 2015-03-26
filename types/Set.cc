/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Set.hh"

#include <sstream>
#include <utility>

#include "common/mica.h"
#include "types/Exceptions.hh"
#include "types/List.hh"
#include "types/Var.hh"

using namespace mica;
using namespace std;

Set::Set() : Data(), var_set() {}

Set::Set(const Set &from) : Data(), var_set(from) {}

Set::Set(const var_set &from) : Data(), var_set(from) {}

Var Set::single(const Var &N) {
  var_set s;
  s.insert(N);
  return new (aligned) Set(s);
}

Var Set::from_set(const var_set &from) { return new (aligned) Set(from); }

var_set Set::as_var_set() const { return *this; }

bool Set::operator==(const Var &rhs) const {
  if (rhs.type_identifier() != type_identifier())
    return false;

  return (Data *)this < rhs->asType<Data *>();
}

bool Set::operator<(const Var &rhs) const {
  if (rhs.type_identifier() != type_identifier())
    return false;

  return (Data *)this < rhs->asType<Data *>();
}

Var Set::add(const Var &rhs) const {
  var_set self(*this);
  self.insert(rhs);

  return new (aligned) Set(self);
}

Var Set::mul(const Var &rhs) const { throw invalid_type("invalid operand"); }

Var Set::sub(const Var &rhs) const { throw invalid_type("invalid operand"); }

Var Set::div(const Var &rhs) const { throw invalid_type("invalid operand"); }

Var Set::insert(const Var &N) const { return add(N); }

Var Set::insertSeq(const Var &N) const {
  var_set self(*this);
  var_vector seq(N.flatten());
  for (var_vector::iterator x = seq.begin(); x != seq.end(); x++) self.insert(*x);

  return new (aligned) Set(self);
}

Var Set::merge(const Var &N) const {
  var_set result;
  insert_iterator<var_set> res_ins(result, result.begin());

  var_vector seq(N.flatten());
  set_union(begin(), end(), seq.begin(), seq.end(), res_ins);

  return new (aligned) Set(result);
}

Var Set::drop(const Var &n) const {
  var_set self(*this);

  var_set::iterator fi = self.find(n);
  if (fi != self.end())
    self.erase(fi);

  return new (aligned) Set(self);
}

Var Set::dropSeq(const Var &n) const {
  var_set self(*this);
  var_vector seq(n.flatten());
  for (var_vector::iterator x = seq.begin(); x != seq.end(); x++) {
    var_set::iterator fi = self.find(*x);
    if (fi != self.end())
      self.erase(fi);
  }
  return new (aligned) Set(self);
}

bool Set::null() const { return this->var_set::empty(); }

int Set::size() const { return (int)this->var_set::size(); }

bool Set::member(const Var &n) const {
  var_set::const_iterator fi(this->var_set::find(n));
  return (fi != end());
}

Var Set::lookup(const Var &n) const {
  var_set::const_iterator fi(this->var_set::find(n));
  if (fi == end())
    throw not_found("set member not found");

  return *fi;
}

Var Set::lookupM(const Var &n) const {
  var_set::const_iterator fi(this->var_set::find(n));
  if (fi == end())
    return NONE;

  return *fi;
}

Var Set::lookup_withDefault(const Var &n, const Var &d) const {
  var_set::iterator fi = find(n);
  if (fi == end())
    return d;

  return *fi;
}

Var Set::intersect(const Var &N) const {
  var_set result;
  insert_iterator<var_set> res_ins(result, result.begin());

  var_vector seq(N.flatten());
  set_intersection(begin(), end(), seq.begin(), seq.end(), res_ins);

  return new (aligned) Set(result);
}

Var Set::difference(const Var &N) const {
  var_set result;
  insert_iterator<var_set> res_ins(result, result.begin());

  var_vector seq(N.flatten());
  set_difference(begin(), end(), seq.begin(), seq.end(), res_ins);

  return new (aligned) Set(result);
}

bool Set::subset(const Var &N) const {
  var_vector seq(N.flatten());
  return includes(begin(), end(), seq.begin(), seq.end());
}

mica_string Set::rep() const {
  mica_string output = "%[";
  var_set::const_iterator si;
  for (si = begin(); si != end();) {
    output.append((*si).rep());
    si++;
    if (si == end())
      break;
    else
      output.append(", ");
  }
  output.push_back(']');

  return output;
}

Var Set::subseq(int, int) const { throw invalid_type("invalid operand"); }

Var Set::cons(const Var &el) const { return List::tuple(Var(this), el); }

Var Set::lhead() const { throw invalid_type("lhead on non-sequence"); }

Var Set::ltail() const { throw invalid_type("ltail on non-sequence"); }

int Set::toint() const { throw invalid_type("invalid operand"); }

float Set::tofloat() const { throw invalid_type("invalid operand"); }

Var Set::mod(const Var &rhs) const { throw invalid_type("invalid operand"); }

Var Set::neg() const { throw invalid_type("invalid operand"); }

mica_string Set::tostring() const { throw invalid_type("invalid operand"); }

void Set::serialize_to(serialize_buffer &s_form) const {
  /** append type name
   */
  Pack(s_form, type_identifier());

  /** write the size
   */
  size_t len = size();
  Pack(s_form, len);

  var_set::const_iterator x;
  for (x = begin(); x != end(); x++) x->serialize_to(s_form);
}

size_t Set::hash() const {
  size_t start = 0;

  var_set::const_iterator x;
  for (x = begin(); x != end(); x++) {
    start += x->hash();
  }

  return start;
}

void Set::append_child_pointers(child_set &child_list) {
  // add each member
  var_set::iterator x;
  for (x = begin(); x != end(); x++) {
    append_data(child_list, *x);
  }
}

var_vector Set::flatten() const {
  var_vector ops;
  ops.insert(ops.end(), this->var_set::begin(), this->var_set::end());

  return ops;
}

var_vector Set::map(const Var &expr) const

{
  /** Finished iterating.  No-op
   */
  if (this->var_set::empty())
    return var_vector();

  /** Assign cdr into variable @ var_index, execute block
   *  continue by iterating the car
   */
  var_vector ops;

  ops.push_back(Var(Op::EVAL));
  ops.push_back(expr);

  var_set car(*this);
  var_set::iterator cdr_it = car.begin();
  ops.push_back(*cdr_it);  // cdr
  car.erase(cdr_it);

  if (size() > 1) {
    ops.push_back(Var(Op::MAP));

    ops.push_back(expr);

    /** car
     */
    ops.push_back(new (aligned) Set(car));
  }

  return ops;
}

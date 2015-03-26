/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Map.hh"

#include <sstream>
#include <utility>

#include "common/mica.h"
#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/List.hh"
#include "types/MetaObjects.hh"
#include "types/Var.hh"

using namespace mica;
using namespace std;

/** These are private
 */
Map::Map() : Data(), var_map() {}

Map::Map(const var_map &from) : Data(), var_map(from) {}

/** Everything below here is public
 */
Var Map::from_map(const var_map &from) {
  if (from.empty())
    return empty();
  else
    return new (aligned) Map(from);
}

bool Map::operator==(const Var &rhs) const {
  if (rhs.type_identifier() != type_identifier())
    return false;

  return *this == *rhs->asType<Map *>();
}

bool Map::operator<(const Var &rhs) const {
  if (rhs.type_identifier() != type_identifier())
    return false;

  return this < rhs->asType<Data *>();
}

Var Map::add(const Var &v2) const { throw unimplemented("map addition"); }

Var Map::sub(const Var &v2) const { throw unimplemented("map subtraction"); }

Var Map::mul(const Var &v2) const { throw unimplemented("map multiplication"); }

Var Map::div(const Var &v2) const { throw unimplemented("map division"); }

Var Map::subseq(int, int) const {
  throw invalid_type("cannot extract subsequence from association");
}

Var Map::cons(const Var &el) const { return List::tuple(Var(this), el); }

Var Map::lhead() const { throw invalid_type("lhead on non-sequence"); }

Var Map::ltail() const { throw invalid_type("ltail on non-sequence"); }

int Map::toint() const { throw invalid_type("invalid operand"); }

float Map::tofloat() const { throw invalid_type("invalid operand"); }

Var Map::mod(const Var &rhs) const { throw invalid_type("invalid operand"); }

Var Map::neg() const { throw invalid_type("invalid operand"); }

Var Map::insert(const Var &K, const Var &V) const {
  var_map new_map(*this);
  new_map.insert(make_pair(K, V));
  return Map::from_map(new_map);
}

Var Map::insertSeq(const Var &N) const {
  var_vector f = N.flatten();
  if (f.size() % 2)
    throw invalid_type("sequence for insert to map is not divided into even pairs");

  var_map new_map(*this);
  var_vector::iterator x = f.begin();
  while (x != f.end()) new_map.insert(make_pair(*x, *x++));

  return Map::from_map(new_map);
}

Var Map::drop(const Var &n) const {
  var_map new_map(*this);
  var_map::iterator f = new_map.find(n);
  if (f == new_map.end())
    throw not_found("key not found");

  new_map.erase(f);

  return Map::from_map(new_map);
}

Var Map::dropSeq(const Var &n) const {
  var_vector flattened = n.flatten();

  var_map new_map(*this);
  for (var_vector::iterator fi = flattened.begin(); fi != flattened.end(); fi++) {
    var_map::iterator f = new_map.find(*fi);
    if (f == new_map.end())
      throw not_found("key not found");

    new_map.erase(f);
  }
  return Map::from_map(new_map);
}

bool Map::null() const { return this->var_map::empty(); }

int Map::size() const { return size(); }

bool Map::member(const Var &n) const { return (find(n) != end()); }

Var Map::lookup(const Var &n) const {
  var_map::const_iterator f = find(n);
  if (f == end())
    throw not_found("key not found");
  else
    return f->second;
}

Var Map::lookupM(const Var &n) const {
  var_map::const_iterator f = find(n);
  if (f == end())
    return NONE;
  else
    return f->second;
}

Var Map::lookup_withDefault(const Var &n, const Var &d) const {
  var_map::const_iterator f = find(n);
  if (f == end())
    return d;
  else
    return f->second;
}

mica_string Map::tostring() const { throw invalid_type("invalid operand"); }

mica_string Map::rep() const {
  mica_string dstr("#[");

  var_map::const_iterator si;
  for (si = begin(); si != end();) {
    dstr.append(si->first.rep());
    dstr.append(" => ");
    dstr.append(si->second.rep());
    si++;
    if (si == end())
      break;
    else
      dstr.append(", ");
  }

  dstr.push_back(']');

  return dstr;
}

void Map::serialize_to(serialize_buffer &s_form) const {
  /** append type name
   */
  Pack(s_form, type_identifier());

  /** write the size
   */
  size_t len = size();
  Pack(s_form, len);

  var_map::const_iterator x;
  for (x = begin(); x != end(); x++) {
    x->first.serialize_to(s_form);
    x->second.serialize_to(s_form);
  }
}

var_vector Map::flatten() const {
  var_vector ops;
  for (var_map::const_iterator x = this->var_map::begin(); x != this->var_map::end(); x++) {
    ops.push_back(x->first);
    ops.push_back(x->second);
  }
  return ops;
}

var_vector Map::map(const Var &expr) const {
  /** Finished iterating.  No-op
   */
  if (this->var_map::empty())
    return var_vector();

  /** Assign cdr into variable @ var_index, execute block
   *  continue by iterating the car
   */
  var_vector ops;

  ops.push_back(Var(Op::EVAL));
  ops.push_back(expr);

  var_map car(*this);
  var_map::iterator cdr_it = car.begin();

  var_vector pair;
  pair.push_back(cdr_it->first);
  pair.push_back(cdr_it->second);

  ops.push_back(List::from_vector(pair));  // cdr

  car.erase(cdr_it);

  if (size() > 1) {
    ops.push_back(Var(Op::MAP));

    ops.push_back(expr);
    /** car
     */
    ops.push_back(new (aligned) Map(car));
  }

  return ops;
}

size_t Map::hash() const {
  size_t start = 0;

  var_map::const_iterator x;
  for (x = begin(); x != end(); x++) {
    start += x->first.hash();
    start += x->second.hash();
  }

  return start;
}

void Map::append_child_pointers(child_set &child_list) {
  var_map::iterator x;
  for (x = begin(); x != end(); x++) {
    append_data(child_list, x->first);
    append_data(child_list, x->second);
  }
}

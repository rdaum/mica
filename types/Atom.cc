/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Atom.hh"

#include <iostream>

#include "types/Exceptions.hh"
#include "types/List.hh"
#include "types/MetaObjects.hh"
#include "types/Var.hh"

namespace mica {

Var Atom::subseq(int start, int length) const {
  throw invalid_type("attempt to extract subseq from scalar operand");
}

Var Atom::lookup(const Var &i) const {
  throw invalid_type("attempt to lookup item inside scalar operand");
}

Var Atom::cons(const Var &el) const { return List::tuple(Var(this), el); }

Var Atom::lhead() const { throw invalid_type("lhead on non-sequence"); }

Var Atom::ltail() const { throw invalid_type("ltail on non-sequence"); }

var_vector Atom::map(const Var &expr) const { throw invalid_type("attempt to map scalar operand"); }

var_vector Atom::flatten() const {
  var_vector ops;
  ops.push_back(Var(this));

  return ops;
}

void Atom::append_child_pointers(child_set &child_list) {}

}  // namespace mica
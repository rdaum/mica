/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/hash.hh"

#include <unordered_map>

#include "base/Ref.hh"

#include "types/Data.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"

using namespace mica;

size_t hash_var::operator()(const Var &var) const { return var.hash(); };

size_t hash_symbol::operator()(const Symbol &sym) const { return sym.hash(); }

size_t str_hash::operator()(const mica_string &str) const {
  std::hash<mica_string> hasher;
  return hasher(str);
};

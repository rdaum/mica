/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "parser/Binding.hh"

#include <algorithm>
#include <vector>

#include "common/mica.h"
#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/Var.hh"

using namespace mica;

void Binding::startBlock() {
  if (bindStack.size()) {
    lastBlockPos.push_back(bindStack.size());
  }
}

unsigned int Binding::finishBlock() {
  if (!lastBlockPos.size())
    return bindStack.size();

  /** Get end of pos for last block
   */
  unsigned int x = lastBlockPos.back();

  unsigned int width = 0;
  /** Unwind
   */
  while (bindStack.size() != x) {
    bindStack.pop_back();
    width++;
  }

  lastBlockPos.pop_back();

  return width;
}

unsigned int Binding::define(const Var &name) {
  bindStack.push_back(name);

  return bindStack.size() - 1;
}

unsigned int Binding::lookup(const Var &name) const {
  BindMap::const_iterator found = std::find(bindStack.begin(), bindStack.end(), name);

  if (found == bindStack.end())
    throw var_not_found("variable not found");
  else
    return found - bindStack.begin();
}

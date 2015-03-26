/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "vm/Environment.hh"

#include <assert.h>
#include <vector>

#include "common/mica.h"
#include "types/Data.hh"
#include "types/Var.hh"
#include "vm/Task.hh"

using namespace mica;
using namespace std;

Environment::Environment() : env(new GCVector()){};

Environment::~Environment() {}

Environment::Environment(const Environment &from) : env(from.env) {}

Environment Environment::copy() const {
  Environment new_env;
  new_env.widths = widths;
  new_env.env = new GCVector(*((GCVector *)env));

  return new_env;
}

void Environment::enter(unsigned int additional) {
  unsigned int old_size = env->size();
  widths.push_back(old_size);
  env->resize(old_size + additional);
}

void Environment::exit() {
  unsigned int old_size = widths.back();
  widths.pop_back();
  env->resize(old_size);
  cerr << "exit" << endl;
}

void Environment::set(unsigned int i, const Var &value) { *(env->begin() + i) = value; }

Var Environment::get(unsigned int i) { return *(env->begin() + i); }

void Environment::serialize_to(serialize_buffer &s_form) const {
  // pack widths
  Pack(s_form, widths.size());
  for (std::vector<unsigned int>::const_iterator x = widths.begin(); x != widths.end(); x++)
    Pack(s_form, *x);

  // pack variables
  Pack(s_form, env->size());
  for (GCVector::const_iterator x = env->begin(); x != env->end(); x++) x->serialize_to(s_form);
}

void Environment::append_child_pointers(child_set &child_list) {
  child_list.push_back((GCVector *)env);
}

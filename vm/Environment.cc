/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"


#include <vector>

#include "Data.hh"
#include "Var.hh"
#include "Task.hh"
#include "Environment.hh"

#include <assert.h>

using namespace mica;
using namespace std;

Environment::Environment() {};

Environment::~Environment() {
}

Environment::Environment( const Environment &from )
  : env(from.env)
{}

void Environment::enter( unsigned int additional )
{
  unsigned int old_size = env.size();
  widths.push_back( old_size );
  env.resize( old_size + additional );
}

void Environment::exit()
{
  unsigned int old_size = widths.back();
  widths.pop_back();
  env.resize( old_size );
  cerr << "exit" << endl;
}

void Environment::set( unsigned int i,
		       const Var &value )
{
  env[i] = value;
}

Var Environment::get( unsigned int i )
{
  return env[i];
}

mica_string Environment::serialize() const
{
  mica_string s_form;

  return s_form;
}

child_set Environment::child_pointers() {
  child_set children;
  append_datas( children, env );
  return children;
}


#include "config.h"

#include "common/mica.h"


#include "Frame.hh"
#include "Closure.hh"


using namespace mica;
using namespace std;

Closure::Closure( const var_vector &i_stack,
		  const Environment &i_scope,
		  const Control &i_control,
		  const ExceptionMap &i_exceptions,
		  ClosureTag i_tag, 
		  const Var &i_self, 
		  const Var &i_definer )
  : stack(i_stack),
    scope(i_scope),
    control(i_control), 
    exceptions(i_exceptions),
    tag(i_tag),
    self(i_self),
    definer(i_definer)
{
}

Closure::~Closure() {}

mica_string Closure::serialize() const {
  return mica_string();
}

child_set Closure::child_pointers() {

  // Control
  child_set children( control.child_pointers() );

  // Stack
  append_datas( children, stack );

  // Environment
  child_set env_childs( scope.child_pointers() );
  children.insert( children.end(), env_childs.begin(), env_childs.end() );

  // Exceptions
  for (ExceptionMap::iterator x = exceptions.begin(); x != exceptions.end();
       x++) {
    children.push_back( (Closure*)x->second.handler );
  }

  children << self << definer;

  return children;
}

mica_string Closure::rep() const {
  return "<closure>";
}

var_vector Closure::perform( const Ref<Frame> &parent, const Var &args ) {
  
  parent->apply_closure( this, args );

  return var_vector();
}



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

void Closure::append_child_pointers( child_set &child_list ) {

  // Control
  control.append_child_pointers( child_list );

  // Stack
  append_datas( child_list, stack );

  // Environment
  scope.append_child_pointers( child_list );

  // Exceptions
  for (ExceptionMap::iterator x = exceptions.begin(); x != exceptions.end();
       x++) {
    child_list.push_back( (Closure*)x->second.handler );
  }

  child_list << self << definer;
}

mica_string Closure::rep() const {
  return "<closure>";
}

var_vector Closure::perform( const Ref<Frame> &parent, const Var &args ) {
  
  parent->apply_closure( this, args );

  return var_vector();
}


#ifndef MICA_CLOSURE_HH
#define MICA_CLOSURE_HH

#include "generic_vm_entity.hh"
#include "Block.hh"
#include "Control.hh"
#include "Environment.hh"
#include "Frame.hh"

namespace mica {
  
  class Closure
    : public generic_vm_entity
  {
  public:
    Type::Identifier type_identifier() const { return Type::CLOSURE; }

  public:
    Closure( const var_vector &i_stack,
	     const Environment &i_scope,
	     const Control &control,
	     const ExceptionMap &i_exceptions,
	     ClosureTag i_tag );

    virtual ~Closure();

  public:
    var_vector stack;

    Environment scope;

    Control control;

    ExceptionMap exceptions;

    ClosureTag tag;

    virtual Var perform( const Ref<Frame> &parent, const Var &args );

  public:
    mica_string serialize() const;
    child_set child_pointers();
    mica_string rep() const;
  };

}

#endif /** MICA_CLOSURE_HH **/

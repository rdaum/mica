#ifndef MICA_CLOSURE_HH
#define MICA_CLOSURE_HH

#include "generic_vm_entity.hh"
#include "Block.hh"
#include "Control.hh"
#include "Environment.hh"
#include "Frame.hh"

namespace mica {

  /** Closure is a snapshot (closure around) state in a Frame, and represents
   *  the current state of a function
   */
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
	     ClosureTag i_tag,
	     const Var &self = NONE,
	     const Var &definer = NONE );

    virtual ~Closure();

  public:
    /** Apply the closure (with arguments) into a running frame
     *  @param frame the running frame in which to apply the closure
     *  @param args arguments to set
     *  @return return value is ignored
     */
    var_vector perform( const Ref<Frame> &frame, const Var &args );

  public:
    var_vector stack;

    Environment scope;

    Control control;

    ExceptionMap exceptions;

    ClosureTag tag;
    
    Var self;

    Var definer;

  public:
    mica_string serialize() const;

    child_set child_pointers();

    mica_string rep() const;
  };

}

#endif /** MICA_CLOSURE_HH **/

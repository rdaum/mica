/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <sstream>

#include "Data.hh"
#include "Var.hh"
#include "NoReturn.hh"
#include "Task.hh"
#include "Data.hh"
#include "Error.hh"
#include "Control.hh"
#include "Exceptions.hh"
#include "Closure.hh"
#include "GlobalSymbols.hh"
#include "List.hh"
#include "Message.hh"
#include "Block.hh"
#include "OpCodes.hh"
#include "Scalar.hh"
#include "Scheduler.hh"
#include "String.hh"
#include "Var.hh"

#include "Frame.hh"

using namespace mica;
using namespace std;

/** Utility macro for calling member functions of objects via ptr
 */
#define cMF(object,ptrToMember)  ((object).*(ptrToMember))


/** Construction, destruction
 *
 */
Frame::Frame( const Ref<Message> &msg, const Var &definer_scope,
	      const Ref<Block> program, int pool_id ) 
  : AbstractFrame(msg, definer_scope, pool_id), 
    executor(execution_visitor(this)), control(program)
{
  ex_state = RUNNING;
  scope.enter( program->add_scope );
}

Frame::Frame()
  : AbstractFrame(), 
    control(Ref<Block>(0)),
    executor(execution_visitor(this))
{}

Frame::Frame( const Ref<Frame> &from )
  : AbstractFrame( (AbstractFrame*)from ), 
    executor(execution_visitor(this)),
    stack(from->stack),
    scope(from->scope),
    exceptions(from->exceptions),
    control(from->control),
    dump(from->dump)
    
{
}


child_set Frame::child_pointers() {

  // What we inherit from an abstract frame
  child_set   child_p( this->AbstractFrame::child_pointers()  );

  // STACK
  append_datas( child_p, stack );

  // ENVIRONMENT
  child_set e_child_p( scope.child_pointers() );
  child_p.insert( child_p.end(), e_child_p.begin(), e_child_p.end() );

  // CONTROL
  child_set c_child_p( control.child_pointers() );
  child_p.insert( child_p.end(), c_child_p.begin(), c_child_p.end() );

  // eXCEPTION
  for (ExceptionMap::iterator x = exceptions.begin(); x != exceptions.end();
       x++) {
    child_p.push_back( (Closure*)x->second );
  }  

  // DUMP
  for (vector<Ref<Closure> >::iterator x = dump.begin(); x != dump.end();
       x++) {
    child_p.push_back( (Closure*)(*x) );
  }  

  return child_p;
}

/** Stack related methods
 *
 */

Var Frame::pop() {
  assert(!stack.empty());
  Var el = stack.back();
  stack.pop_back();
  return el;
}


void Frame::push(const Var &v)
{
  stack.push_back(v);
}

Var Frame::pop_exec() {
  assert(!control.exec_stack.empty());
  Var el = control.exec_stack.back();
  control.exec_stack.pop_back();
  return el;
}


void Frame::push_exec(const Var &v)
{
  control.exec_stack.push_back(v);
}



void Frame::push_dump( const Ref<Closure> &closure ) {
  dump.push_back(closure);
}

Ref<Closure> Frame::make_closure( ClosureTag tag ) const {
  return new Closure( stack, scope, control, exceptions, tag );
}

void Frame::load_closure( const Ref<Closure> &closure, bool mutable_scope ) {

  /** Restore a closure
   */
  stack = closure->stack;

  if (!mutable_scope)         // Closure cannot mutate parent's environment
    scope = closure->scope;  
  else                        // We use the same environment, just resized
    scope.env.resize( closure->scope.env.size() );

  control = closure->control;
  exceptions = closure->exceptions;
}

void Frame::switch_branch( const Ref<Block> &switch_to ) {
  push_dump( make_closure( BRANCH ) );
  control = Control( switch_to );
  scope.enter( switch_to->add_scope );
}

void Frame::loop_begin( const Ref<Block> &loop_expr ) {

  /** Push the outside of the loop
   */
  push_dump( make_closure(LOOP_OUTSIDE) );

  /** Enter the loop
   */
  control = Control( loop_expr );

  /** Push the inside of the loop
   */
  push_dump( make_closure(LOOP_INSIDE) );

  scope.enter( loop_expr->add_scope );
}

void Frame::loop_break() {

  /** Restore to outside of the loop
   */
  while (!dump.empty()) {
    Ref<Closure> entry = dump.back();
    dump.pop_back();
    if (entry->tag == LOOP_OUTSIDE) { 
      load_closure( entry );
      break;
    }
  }
}

void Frame::loop_continue() {

  /** Restore to start of inside of loop
   */
  while (!dump.empty()) {
    Ref<Closure> entry = dump.back();
    if (entry->tag == LOOP_INSIDE) {
      load_closure( entry );
      control.reset();
      break;
    }
    dump.pop_back();
  }
}



void Frame::apply_closure( const Ref<Closure> &closure, 
			   const Var &arguments ) {
  
  push_dump( make_closure() );

  load_closure( closure, false );

  args = arguments.flatten();
}

bool Frame::receive_exception( const Ref<Error> &error )
{
  /** Need to walk back the dump and look for exception handlers
   *  in the closures there that match the error.  Upon finding
   *  one, we restore that closure and push the error to the
   *  stack.  If we don't find one, we return false, and the
   *  error propagates up to the calling frame.
   */
  return false;
}


/** Task activation handling
 *
 */

void Frame::handle_message( const Ref<Message> &reply_message )
{     
  if ( reply_message->isReturn() ) {

    /** We've received a return value.  Push it to the stack.
     */
    push( reply_message->args[0] );

  } else if ( reply_message->isRaise() ) {

    Ref<RaiseMessage> raise_message = reply_message->asRef<RaiseMessage>();
    
    resume_raise( raise_message->error(), raise_message->traceback() );

  } else if ( reply_message->isHalt() ) {

    halt();
	  
  } 

}

/** Execute status related methods
 */
bool Frame::is_terminated() 
{
  if (terminated) {

    raise( mica::terminated("task terminated") );

    return true;
  } else
    return false;
}

void Frame::stop()
{
  ex_state = STOPPED;
}

void Frame::run()
{
  ex_state = RUNNING;
}


void Frame::halt()
{
  ex_state = HALTED;
}

/** Virtual machine execution
 */

void Frame::resume()
{
  ex_state = RUNNING;

  execute();
}

void Frame::execute_opcode( const Op &op ) 
{
  if (op.code > Op::MAP_MARKER) {
    OpInfo *x = opcodes[op.code];
    //    cerr << x->name << " ";
    cMF( *this, x->func )( op.param_1, op.param_2 );
  } else {
    //    cerr << Var(op) << " ";
    push(Var(op));
  }
}

void Frame::execute()
{
  try {
    while (ex_state == RUNNING) {
    
      /** If there's instructions in the exec stack, execute them first
       */
      while (!control.exec_stack.empty()) {

	pop_exec().apply_visitor<void>( executor );

	tick();
      }

      /** Check to see if there's any operations left in the current
       *  expression.  If not, we pop the dump stack and continue from
       *  there, if possible.  
       *  If that fails, mark this frame as terminated and end, 
       *  presumably giving control back to the scheduler, which should
       *  delete this frame and get the next one.
       */
      while (control.finished()) {
	if ( ! dump.empty() ) {                 // Returning from a function,
	  Ref<Closure> restore = dump.back();   // branch or loop?
	  restore->stack.push_back( stack.back() );
	  dump.pop_back();
	  load_closure( restore );
	  continue;
	} else {                         // Nothing left, return top of
                                         // stack or NONE if stack is empty.
	  reply_return( stack.empty() ? NONE : stack.back() );
	  terminate();

	  return;
	}
      }

      /** Otherwise, push the next opcode in the program to the
       *  execution stack
       */
      push_exec( control.next_opcode() );

    }
  } catch (const Ref<Error> &err) {

    raise( err );

  }


}

Var Frame::next() {
  
  if (!control.exec_stack.empty()) 
    return pop_exec();
  else
    return control.next_opcode();

}

void Frame::resume_raise( const Ref<Error> &error, mica_string trace_str )
{
  /** Check to see if this frame can handle the error.
   *  If it can't, we add to the traceback and pass it upwards.
   */
  if (!receive_exception( error )) {

    /** Add our information to the traceback itself.
     */
    trace_str.append( traceback() );

    /** Stop the machine from executing this method.
     */
    ex_state = RAISED;	

    reply_raise( error, trace_str );

    /** Kill the frame that sent it, since it couldn't handle it.
     */
    terminate();

    /** Suspend the machine to let the scheduler do the work
     */
    ex_state = RAISED;

  } else {

    run();

  }

}

void Frame::raise( const Ref<Error> &err )
{
  push( Var(err) );

  if (!receive_exception( err )) {
    
    /** Construct traceback.
     */
    mica_string errstr(err->rep());
    errstr.append( traceback() );

    reply_raise( err, errstr );

    /** Since we couldn't handle it, terminate.
     */
    terminate();
    
    /** Suspend the machine to let the scheduler do the work
     */
    ex_state = RAISED;
    
  } else {
    
    run();
    
  }
}

mica_string Frame::rep() const {
  mica_string out("<frame ");
  out.append(self.rep());
  out.push_back(':');
  out.append(selector.tostring());
  out.push_back('>');

  return out;
}

mica_string Frame::traceback() const
{

  std::ostringstream tstr;

  /** Calculate the line # by walking PC offsets in the block
   */
  unsigned int lineno = control.current_line();

  tstr << " in " << definer;
  tstr << ":" << selector.tostring();
  tstr << " on " << self;
  if (lineno)
    tstr << ", line " << lineno;

#ifndef OSTRSTREAM_APPENDS_NULLS
  tstr << std::ends;
#endif


  return mica_string(tstr.str().c_str());
}


Var Frame::perform( const Ref<Frame> &parent, const Var &new_args ) {

  /** If the existing parent isn't null, then this task can't be
   *  manipulated.  Note that objects should have their parent_task
   *  set to NULL after sending their replies to their parent_task
   */
  if ((Task*)parent_task != 0 && (Task*)parent_task != (Task*)parent)
    throw permission_error("blocked");

  /** Make sure the scheduler doesn't already have this task.  If it does,
   *  if it does, it's not finished yet.
   */
  if (Scheduler::instance->has_task( this ))
    throw permission_error("already scheduled");

  if ((Task*)parent != (Task*)parent_task)
    parent_task = parent;

  /** Block the parent on it.
   */
  Ref<Message> invoking_msg( parent->spawn() );

  /** Set the msg_id
   */
  msg_id = invoking_msg->msg_id;

  /** Assign the arguments
   */
  args = new_args.flatten();

  /** This is a frame, not a continuation -- restart it from the beginning
   *  each time.  That means resetting the execution context's PC and toggling
   *  terminated to false.
   */
  terminated = false;
  control._pc = -1;

  /** Add to the scheduler for performance.
   */
  Scheduler::instance->event_add( this );

  return NoReturn::instance;
}

mica_string Frame::serialize_full() const {
  mica_string s_form( this->AbstractFrame::serialize_full() );

  s_form.append( control.serialize() );

  Pack( s_form, stack.size() );
  for (var_vector::const_iterator x = stack.begin();
       x != stack.end(); x++) {
    s_form.append( x->serialize() );
  }

  s_form.append( scope.serialize() );


  Pack( s_form, ex_state );
  
  return s_form;

}

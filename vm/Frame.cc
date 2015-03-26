/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "vm/Frame.hh"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <sstream>


#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Data.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/List.hh"
#include "types/String.hh"
#include "types/Var.hh"
#include "types/Var.hh"
#include "vm/Block.hh"
#include "vm/Closure.hh"
#include "vm/Control.hh"
#include "vm/Message.hh"
#include "vm/OpCodes.hh"
#include "vm/Scheduler.hh"
#include "vm/Task.hh"

using namespace mica;
using namespace std;

/** Utility macro for calling member functions of objects via ptr
 */
#define cMF(object, ptrToMember) ((object).*(ptrToMember))

ExceptionHandler::ExceptionHandler(uint16_t i_var_idx, const Ref<Closure> &i_handler)
    : var_idx(i_var_idx), handler(i_handler) {}

ExceptionHandler::ExceptionHandler(const ExceptionHandler &xc)
    : var_idx(xc.var_idx), handler(xc.handler) {}

ExceptionHandler &ExceptionHandler::operator=(const ExceptionHandler &rhs) {
  if (&rhs != this) {
    var_idx = rhs.var_idx;
    handler = rhs.handler;
  }
  return *this;
}

bool ExceptionHandler::operator==(const ExceptionHandler &rhs) const {
  return (&rhs == this) || (handler == rhs.handler && var_idx == rhs.var_idx);
}

void ExceptionHandler::serialize_to(serialize_buffer &s_form) const {
  Pack(s_form, var_idx);
  handler->serialize_to(s_form);
}

/** Construction, destruction
 *
 */
Frame::Frame(const Ref<Message> &msg, const Var &definer_scope, const Ref<Block> program,
             int pool_id)
    : Task(msg->parent_task, msg->msg_id, pool_id),
      selector(NONE_SYM),
      definer(definer_scope),
      executor(execution_visitor(this)),
      control(program) {
  prepare(msg);

  ex_state = RUNNING;
  scope.enter(program->add_scope);
}

Frame::Frame() : Task(), selector(), executor(execution_visitor(this)), control(Ref<Block>(0)) {}

Frame::Frame(const Ref<Frame> &from)
    : Task((Task *)from),
      source(from->source),
      caller(from->caller),
      self(from->self),
      on(from->on),
      selector(from->selector),
      definer(from->definer),
      args(from->args),
      executor(execution_visitor(this)),
      stack(from->stack),
      scope(from->scope),
      exceptions(from->exceptions),
      control(from->control),
      dump(from->dump)

{}
void Frame::prepare(const Ref<Message> &msg) {
  /** Copy all the context from the message.
   */
  age = msg->age;
  source = msg->source;
  caller = msg->caller;
  self = msg->self;
  selector = msg->selector;
  args = msg->args;
}

void Frame::append_child_pointers(child_set &child_list) {
  child_list << self << caller << source << definer << on;

  append_datas(child_list, args);

  // STACK
  append_datas(child_list, stack);

  // ENVIRONMENT
  scope.append_child_pointers(child_list);

  // CONTROL
  control.append_child_pointers(child_list);

  // eXCEPTION
  for (ExceptionMap::iterator x = exceptions.begin(); x != exceptions.end(); x++) {
    child_list.push_back((Closure *)x->second.handler);
  }

  // DUMP
  for (vector<Ref<Closure> >::iterator x = dump.begin(); x != dump.end(); x++) {
    child_list.push_back((Closure *)(*x));
  }
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

void Frame::push(const Var &v) { stack.push_back(v); }

Var Frame::pop_exec() {
  assert(!control.exec_stack.empty());
  Var el = control.exec_stack.back();
  control.exec_stack.pop_back();
  return el;
}

void Frame::push_exec(const Var &v) { control.exec_stack.push_back(v); }

void Frame::push_dump(const Ref<Closure> &closure) { dump.push_back(closure); }

Ref<Closure> Frame::make_closure(ClosureTag tag) const {
  return new Closure(stack, scope, control, exceptions, tag, self, definer);
}

void Frame::load_closure(const Ref<Closure> &closure) {
  /** Restore a closure
   */
  stack = closure->stack;
  scope = closure->scope;
  control = closure->control;
  exceptions = closure->exceptions;

  /** If the closure stores self and definer, load them.
   *  Note that anonymous functions are always created with NONE
   *  values for self + definer, thus they are always executed with
   *  the self+definer value of the person applying them, not the
   *  creator of them.
   */

  if (closure->self != NONE)
    self = closure->self;

  if (closure->definer != NONE)
    definer = closure->definer;
}

void Frame::switch_branch(const Ref<Block> &switch_to) {
  push_dump(make_closure(BRANCH));
  control = Control(switch_to);
  scope.enter(switch_to->add_scope);
}

void Frame::loop_begin(const Ref<Block> &loop_expr) {
  /** Push the outside of the loop
   */
  push_dump(make_closure(LOOP_OUTSIDE));

  /** Enter the loop
   */
  control = Control(loop_expr);

  /** Push the inside of the loop
   */
  push_dump(make_closure(LOOP_INSIDE));

  scope.enter(loop_expr->add_scope);
}

void Frame::loop_break() {
  /** Restore to outside of the loop
   */
  while (!dump.empty()) {
    Ref<Closure> entry = dump.back();
    dump.pop_back();
    if (entry->tag == LOOP_OUTSIDE) {
      load_closure(entry);
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
      load_closure(entry);
      control.reset();
      break;
    }
    dump.pop_back();
  }
}

void Frame::apply_closure(const Ref<Closure> &closure, const Var &arguments) {
  push_dump(make_closure());

  load_closure(closure);

  args = arguments.flatten();
}

bool Frame::handle_exception(const Ref<Error> &error) {
  /** Need to walk back the dump and look for exception handlers
   *  in the closures there that match the error.  Upon finding
   *  one, we restore that closure and push the error to the
   *  stack.  If we don't find one, we return false, and the
   *  error propagates up to the calling frame.
   */

  while (!dump.empty()) {
    Ref<Closure> backup = dump.back();
    dump.pop_back();
    ExceptionMap::iterator exc = backup->exceptions.find(error);

    if (exc != backup->exceptions.end()) {
      load_closure(exc->second.handler);
      scope.set(exc->second.var_idx, Var(error));
      return true;
    }
  }
  return false;
}

/** Task activation handling
 *
 */

void Frame::handle_message(const Ref<Message> &reply_message) {
  if (reply_message->isReturn()) {
    /** We've received a return value.  Push it to the stack.
     */
    push(reply_message->args[0]);

  } else if (reply_message->isRaise()) {
    Ref<RaiseMessage> raise_message = reply_message->asRef<RaiseMessage>();

    resume_raise(raise_message->error(), raise_message->traceback());

  } else if (reply_message->isHalt()) {
    halt();
  }
}

/** Execute status related methods
 */
bool Frame::is_terminated() {
  if (terminated) {
    raise(mica::terminated("task terminated"));

    return true;
  } else
    return false;
}

void Frame::stop() { ex_state = STOPPED; }

void Frame::run() { ex_state = RUNNING; }

void Frame::halt() { ex_state = HALTED; }

/** Virtual machine execution
 */

void Frame::resume() {
  ex_state = RUNNING;

  execute();
}

void Frame::execute_opcode(const Op &op) {
  if (op.code > Op::MAP_MARKER) {
    OpInfo *x = opcodes[op.code];
    //    cerr << x->name << " ";
    cMF (*this, x->func)(op.param_1, op.param_2);
  } else {
    //    cerr << Var(op) << " ";
    push(Var(op));
  }
}

void Frame::execute() {
  try {
    while (ex_state == RUNNING) {
      /** If there's instructions in the exec stack, execute them first
       */
      while (!control.exec_stack.empty()) {
        pop_exec().apply_visitor<void>(executor);

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
        if (!dump.empty()) {                   // Returning from a function,
          Ref<Closure> restore = dump.back();  // branch or loop?
          restore->stack.push_back(stack.back());
          dump.pop_back();
          load_closure(restore);
          continue;
        } else {  // Nothing left, return top of
                  // stack or NONE if stack is empty.
          reply_return(stack.empty() ? NONE : stack.back());
          terminate();

          return;
        }
      }

      /** Otherwise, push the next opcode in the program to the
       *  execution stack
       */
      push_exec(control.next_opcode());
    }
  } catch (const Ref<Error> &err) {
    raise(err);
  }
}

Var Frame::next() {
  if (!control.exec_stack.empty())
    return pop_exec();
  else
    return control.next_opcode();
}

void Frame::resume_raise(const Ref<Error> &error, mica_string trace_str) {
  /** Check to see if this frame can handle the error.
   *  If it can't, we add to the traceback and pass it upwards.
   */
  if (!handle_exception(error)) {
    /** Add our information to the traceback itself.
     */
    trace_str.append(traceback());

    /** Stop the machine from executing this method.
     */
    ex_state = RAISED;

    reply_raise(error, trace_str);

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

void Frame::raise(const Ref<Error> &err) {
  push(Var(err));

  if (!handle_exception(err)) {
    /** Construct traceback.
     */
    mica_string errstr(err->rep());
    errstr.append(traceback());

    reply_raise(err, errstr);

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

mica_string Frame::traceback() const {
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

void Frame::serialize_full_to(serialize_buffer &s_form) const {
  this->Task::serialize_full_to(s_form);

  source.serialize_to(s_form);
  caller.serialize_to(s_form);
  self.serialize_to(s_form);
  on.serialize_to(s_form);

  s_form.append(selector.serialize());

  definer.serialize_to(s_form);

  Pack(s_form, args.size());

  SerializeVV(s_form, args);

  // STACK
  SerializeVV(s_form, stack);

  // SCOPE
  scope.serialize_to(s_form);

  // EXCEPTIONS
  Pack(s_form, exceptions.size());
  for (ExceptionMap::const_iterator x = exceptions.begin(); x != exceptions.end(); x++) {
    x->first->serialize_to(s_form);
    x->second.serialize_to(s_form);
  }
  // CONTROL
  control.serialize_to(s_form);

  // DUMP
  Pack(s_form, dump.size());
  for (std::vector<Ref<Closure> >::const_iterator x = dump.begin(); x != dump.end(); x++)
    (*x)->serialize_to(s_form);

  Pack(s_form, ex_state);
}

void Frame::reply_return(const Var &value) {
  var_vector arguments;
  arguments.push_back(value);
  reply(new
            ReturnMessage(this, msg_id, age, ticks, source, caller, self, on, selector, arguments));
}

void Frame::reply_raise(const Ref<Error> &error, mica_string traceback) {
  reply(new RaiseMessage(this, msg_id, age, ticks, source, caller, self, on, selector,
                                   error, traceback));
}

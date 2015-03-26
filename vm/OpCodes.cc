/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "vm/OpCodes.hh"

#include <assert.h>
#include <iostream>
#include <utility>
#include <vector>

#include "common/mica.h"
#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/List.hh"
#include "types/Map.hh"
#include "types/Object.hh"
#include "types/Set.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "types/Workspace.hh"
#include "types/Workspaces.hh"
#include "vm/Block.hh"
#include "vm/Closure.hh"
#include "vm/Frame.hh"
#include "vm/Frame.hh"
#include "vm/Message.hh"
#include "vm/Scheduler.hh"
#include "vm/Slots.hh"

using namespace std;
using namespace mica;

OpInfo opcode_info[] = {
    {Op::LIST_MARKER, "LIST_MARKER", &Frame::op_fail, 0},
    {Op::SET_MARKER, "SET_MARKER", &Frame::op_fail, 0},
    {Op::MAP_MARKER, "MAP_MARKER", &Frame::op_fail, 0},
    {Op::POP_LIST, "POP_LIST", &Frame::op_pop_list, 0},
    {Op::POP_SET, "POP_SET", &Frame::op_pop_set, 0},
    {Op::POP_MAP, "POP_MAP", &Frame::op_pop_map, 0},
    {Op::POP, "POP", &Frame::op_pop, 0},
    {Op::FLATTEN, "FLATTEN", &Frame::op_flatten, 0},

    {Op::SLICE, "SLICE", &Frame::op_slice, 0},
    {Op::GETRANGE, "GETRANGE", &Frame::op_getrange, 0},

    {Op::SETVAR, "SETVAR", &Frame::op_setvar, 1},
    {Op::GETVAR, "GETVAR", &Frame::op_getvar, 1},

    {Op::SCATTER, "SCATTER", &Frame::op_scatter, 1},

    {Op::SLOTS, "SLOTS", &Frame::op_slots, 0},

    {Op::GETPRIVATE, "GETPRIVATE", &Frame::op_getprivate, 0},
    {Op::DECLPRIVATE, "DECLPRIVATE", &Frame::op_declprivate, 0},
    {Op::SETPRIVATE, "SETPRIVATE", &Frame::op_setprivate, 0},
    {Op::RMPRIVATE, "RMPRIVATE", &Frame::op_rmprivate, 0},

    {Op::GETVERB, "GETVERB", &Frame::op_getverb, 0},
    {Op::DECLVERB, "DECLVERB", &Frame::op_declverb, 0},
    {Op::SETVERB, "SETVERB", &Frame::op_setverb, 0},
    {Op::RMVERB, "RMVERB", &Frame::op_rmverb, 0},

    {Op::DECLNAME, "DECLNAME", &Frame::op_declname, 1},
    {Op::RMNAME, "RMNAME", &Frame::op_rmname, 0},
    {Op::SETNAME, "SETNAME", &Frame::op_setname, 0},
    {Op::GETNAME, "GETNAME", &Frame::op_getname, 1},

    {Op::GETDELEGATE, "GETDELEGATE", &Frame::op_getdelegate, 0},
    {Op::DECLDELEGATE, "DECLDELEGATE", &Frame::op_decldelegate, 0},
    {Op::SETDELEGATE, "SETDELEGATE", &Frame::op_setdelegate, 0},
    {Op::RMDELEGATE, "RMDELEGATE", &Frame::op_rmdelegate, 0},

    {Op::MAKE_OBJECT, "MAKE_OBJECT", &Frame::op_make_object, 1},
    {Op::DESTROY, "DESTROY", &Frame::op_destroy, 0},
    {Op::SEND, "SEND", &Frame::op_send, 0},
    {Op::SEND_LIKE, "SEND_LIKE", &Frame::op_send_like, 0},
    {Op::PERFORM, "PERFORM", &Frame::op_perform, 0},
    {Op::NOTIFY, "NOTIFY", &Frame::op_notify, 0},
    {Op::DETACH, "DETACH", &Frame::op_detach, 0},
    {Op::PASS, "PASS", &Frame::op_pass, 0},
    {Op::RETURN, "RETURN", &Frame::op_return, 0},
    {Op::TICKS, "TICKS", &Frame::op_ticks, 0},
    {Op::CALLER, "CALLER", &Frame::op_caller, 0},
    {Op::SELF, "SELF", &Frame::op_self, 0},
    {Op::SUSPEND, "SUSPEND", &Frame::op_suspend, 0},
    {Op::SOURCE, "SOURCE", &Frame::op_source, 0},
    {Op::SELECTOR, "SELECTOR", &Frame::op_selector, 0},
    {Op::ARGS, "ARGS", &Frame::op_args, 0},
    {Op::NOT, "NOT", &Frame::op_not, 0},
    {Op::NEG, "NEG", &Frame::op_neg, 0},
    //   { Op::POS,	"POS",		&Frame::op_pos, 0 },
    //   { Op::ABS,	"ABS",		&Frame::op_abs, 0 },
    {Op::LSHIFT, "LSHIFT", &Frame::op_lshift, 0},
    {Op::RSHIFT, "RSHIFT", &Frame::op_rshift, 0},
    {Op::BAND, "BAND", &Frame::op_band, 0},
    {Op::BOR, "BOR", &Frame::op_bor, 0},
    {Op::XOR, "XOR", &Frame::op_xor, 0},
    {Op::AND, "AND", &Frame::op_and, 0},
    {Op::OR, "OR", &Frame::op_or, 0},
    {Op::CDR, "CDR", &Frame::op_cdr, 0},
    {Op::CONS, "CONS", &Frame::op_cons, 0},
    {Op::CAR, "CAR", &Frame::op_car, 0},
    {Op::ADD, "ADD", &Frame::op_add, 0},
    {Op::SUB, "SUB", &Frame::op_sub, 0},
    {Op::MUL, "MUL", &Frame::op_mul, 0},
    {Op::DIV, "DIV", &Frame::op_div, 0},
    {Op::MOD, "MOD", &Frame::op_mod, 0},
    {Op::ISA, "ISA", &Frame::op_isa, 0},
    {Op::EQUAL, "EQUAL", &Frame::op_equal, 0},
    {Op::NEQUAL, "NEQAL", &Frame::op_nequal, 0},
    {Op::LESST, "LESST", &Frame::op_lesst, 0},
    {Op::LESSTE, "LESSTE", &Frame::op_lesste, 0},
    {Op::GREATERT, "GREATERT", &Frame::op_greatert, 0},
    {Op::GREATERTE, "GREATERTE", &Frame::op_greaterte, 0},

    /** Frame creation, block evaluation
     */
    {Op::CLOSURE, "CLOSURE", &Frame::op_closure, 0},
    {Op::J, "J", &Frame::op_j, 0},
    {Op::MAKE_LAMBDA, "MAKE_LAMBDA", &Frame::op_make_lambda, 0},
    {Op::EVAL, "EVAL", &Frame::op_eval, 0},

    /** Looping constructs
     */
    {Op::BREAK, "BREAK", &Frame::op_break, 0},
    {Op::CONTINUE, "CONTINUE", &Frame::op_continue, 0},
    {Op::MAP, "MAP", &Frame::op_map, 0},

    {Op::LOOP, "LOOP", &Frame::op_loop, 1},

    /** Exceptions
     */
    {Op::THROW, "THROW", &Frame::op_throw, 0},
    {Op::CATCH, "CATCH", &Frame::op_catch, 0},

    /** IF[/ELSE]
     */
    {Op::JOIN, "JOIN", &Frame::op_join, 0},
    {Op::TRAMPOLINE, "TRAMPOLINE", &Frame::op_trampoline, 0},
    {Op::IF, "IF", &Frame::op_if, 0},
    {Op::IFELSE, "IFELSE", &Frame::op_ifelse, 0},

};

vector<OpInfo *> mica::opcodes;

void mica::initializeOpcodes() {
  size_t i;

  opcodes.resize(LAST_TOKEN + 1);
  for (i = 0; i < (sizeof(opcode_info) / sizeof(OpInfo)); i++) {
    if (opcode_info[i].code >= 0)
      opcodes[opcode_info[i].code] = &opcode_info[i];
  }
}

void Frame::op_fail(unsigned int param_1, unsigned int param_2) {
  // Should never get here.

  assert(0);
}

void Frame::op_neg(unsigned int param_1, unsigned int param_2) {
  Var val(pop());

  push((-val));
}

void Frame::op_cdr(unsigned int param_1, unsigned int param_2) {
  Var el(pop());

  push(el.ltail());
}

void Frame::op_car(unsigned int param_1, unsigned int param_2) {
  Var el(pop());

  push(el.lhead());
}

void Frame::op_cons(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left.cons(right));
}

void Frame::op_add(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left + right);
}

void Frame::op_sub(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left - right);
}

void Frame::op_mul(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left * right);
}

void Frame::op_div(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left / right);
}

void Frame::op_mod(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left % right);
}

void Frame::op_isa(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(Slots::isA(left, right)));
}

void Frame::op_equal(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(left == right));
}

void Frame::op_nequal(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(left != right));
}

void Frame::op_lesst(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(left < right));
}

void Frame::op_greatert(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(left > right));
}

void Frame::op_greaterte(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(left >= right));
}

void Frame::op_lesste(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(Var(left <= right));
}

void Frame::op_and(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left && right);
}

void Frame::op_or(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left || right);
}

void Frame::op_xor(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left ^ right);
}

void Frame::op_lshift(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left << right);
}

void Frame::op_rshift(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left >> right);
}

void Frame::op_band(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left & right);
}

void Frame::op_bor(unsigned int param_1, unsigned int param_2) {
  Var left(pop());
  Var right(pop());

  push(left | right);
}

void Frame::op_not(unsigned int param_1, unsigned int param_2) {
  Var val(pop());

  push(Var(!val));
}

void Frame::op_return(unsigned int param_1, unsigned int param_2) {
  reply_return(pop());

  terminate();

  ex_state = STOPPED;
}

void Frame::op_eval(unsigned int param_1, unsigned int param_2) {
  /** Pop the block from the stack
   */
  Var block(pop());

  /** Verify that it's a block
   */
  if (!block.isBlock())
    throw invalid_type("attempt to evaluate non-block");

  switch_branch(block->asRef<Block>());
}

void Frame::op_trampoline(unsigned int param_1, unsigned int param_2) {
  /** Pop the block from the stack
   */
  Var block(pop());

  /** Verify that it's a block
   */
  if (!block.isBlock())
    throw invalid_type("attempt to evaluate non-block");

  control = Control(block->asRef<Block>());
}

void Frame::op_closure(unsigned int param_1, unsigned int param_2) { push(Var(make_closure())); }

void Frame::op_j(unsigned int param_1, unsigned int param_2) {
  Var clos(pop());
  load_closure(clos->asRef<Closure>());
}

void Frame::op_make_lambda(unsigned int param_1, unsigned int param_2) {
  /** Pop the block
   */
  Var block(pop());

  /** Verify it is a block
   */
  if (!block.isBlock())
    throw invalid_type("attempt to create a lambda from a non-block");

  Ref<Block> block_obj = block->asRef<Block>();

  /** Now create a closure for it
   */
  Ref<Closure> lambda = new Closure(var_vector(),        // Empty stack
                                    scope.copy(),        // Inherit the scope
                                    Control(block_obj),  // The block
                                    ExceptionMap(),      // New exception map
                                    CLOSURE);
  push(Var(lambda));
}

void Frame::op_make_object(unsigned int param_1, unsigned int param_2) {
  /** Pop the constructor block
   */
  Ref<Block> c_block(pop()->asRef<Block>());

  /** Build a closure for it
   */
  Ref<Closure> cl = new Closure(var_vector(),      // Empty stack
                                scope.copy(),      // Inherit the scope
                                Control(c_block),  // The block
                                ExceptionMap(),    // New exception map
                                CLOSURE);

  /** Expand its variable scope by 1, to make room for "creator"
   */
  cl->scope.enter(1);

  /** Set that variable to current self
   */
  cl->scope.set(param_1, self);

  /** Now create an object and put it on self + definer on the new frame
   */
  Var new_object(Object::create());
  cl->self = new_object;
  cl->definer = new_object;

  push(Var(cl));
}

void Frame::op_catch(unsigned int param_1, unsigned int param_2) {
  /** Pop error
   */
  Var err(pop());

  /** Pop block
   */
  Ref<Block> block_obj(pop()->asRef<Block>());

  /** Make the closure for the block
   */
  Ref<Closure> handler = new Closure(var_vector(),        // Empty stack
                                     scope,               // Inherit the scope
                                     Control(block_obj),  // The block
                                     ExceptionMap(),      // New exception map
                                     CLOSURE);

  cerr << "Inserting handler for " << err << " => " << Var(block_obj) << endl;
  exceptions.insert(make_pair(err->asRef<Error>(), ExceptionHandler(param_1, handler)));
}

void Frame::op_perform(unsigned int param_1, unsigned int param_2) {
  Var args(pop()), lhs(pop());

  var_vector result(lhs.perform(this, args));

  /** Append the return results to the exec_stack.  Allows for
   *  the return of continuations.
   */
  control.exec_stack.insert(control.exec_stack.end(), result.begin(), result.end());
}

void Frame::op_self(unsigned int param_1, unsigned int param_2) { push(Var(self)); }

void Frame::op_continue(unsigned int param_1, unsigned int param_2) { loop_continue(); }

void Frame::op_break(unsigned int param_1, unsigned int param_2) { loop_break(); }

void Frame::op_map(unsigned int param_1, unsigned int param_2) {
  /** pop the expr
   */
  Var expr(pop());

  /** pop the sequence
   */
  Var sequence(pop());

  /** Build the instructions for iteration..
   */
  var_vector ops(sequence.map(expr));

  /** Schedule them for execution.
   */
  for (var_vector::iterator x = ops.begin(); x != ops.end(); x++) {
    push_exec(*x);
  }
}

void Frame::op_join(unsigned int param_1, unsigned int param_2) {
  /** Restore to start of inside of loop
   */
  while (!dump.empty()) {
    Ref<Closure> entry = dump.back();
    dump.pop_back();
    if (entry->tag == BRANCH) {
      load_closure(entry);
      break;
    }
  }
}

void Frame::op_if(unsigned int param_1, unsigned int param_2) {
  /** Pop success branch
   */
  Var success(pop());

  /** Pop truth test
   */
  Var tt(pop());

  /** If truth test succeeded, push success opcodes.
   *  Otherwise... push fail opcodes
   */
  if (tt.truth())
    switch_branch(success->asRef<Block>());
}

void Frame::op_ifelse(unsigned int param_1, unsigned int param_2) {
  /** Pop fail branch
   */
  Var fail(pop());

  /** Pop success branch
   */
  Var success(pop());

  /** Pop truth test
   */
  Var tt(pop());

  /** If truth test succeeded, push success opcodes.
   *  Otherwise... push fail opcodes
   */
  if (tt.truth())
    switch_branch(success->asRef<Block>());
  else
    switch_branch(fail->asRef<Block>());
}

void Frame::op_loop(unsigned int param_1, unsigned int param_2) {
  Var block(pop());
  loop_begin(block->asRef<Block>());
}

void Frame::op_throw(unsigned int param_1, unsigned int param_2) {
  /** Pop error
   */
  Var err(pop());

  if (err.type_identifier() != Type::ERROR)
    throw invalid_type("attempt to throw non-error type");

  /** Raise it
   */
  raise(err->asRef<Error>());
}

void Frame::op_pop_list(unsigned int param_1, unsigned int param_2) {
  var_vector lst;

  Var element;

  while ((element = pop()).type_identifier() != Type::OPCODE) {
    lst.push_back(element);
  }

  if (element != Var(Op::LIST_MARKER))
    throw internal_error("stack underflow in pop_list");

  push(List::from_vector(lst));
}

void Frame::op_pop_set(unsigned int param_1, unsigned int param_2) {
  var_set new_set;

  Var element;

  while ((element = pop()).type_identifier() != Type::OPCODE) {
    new_set.insert(element);
  }

  if (element != Var(Op::SET_MARKER))
    throw internal_error("stack underflow in pop_set");

  push(Set::from_set(new_set));
}

void Frame::op_pop_map(unsigned int param_1, unsigned int param_2) {
  Var left, right;
  var_map x;

  while ((right = pop()).type_identifier() != Type::OPCODE) {
    Var left(pop());
    x[left] = right;
  }

  if (right != Var(Op::MAP_MARKER))
    throw internal_error("stack underflow in pop_map");

  push(Map::from_map(x));
}

void Frame::op_pop(unsigned int param_1, unsigned int param_2) { pop(); }

void Frame::op_flatten(unsigned int param_1, unsigned int param_2) {
  Var x = pop();

  var_vector ops(x.flatten());
  for (var_vector::iterator x = ops.begin(); x != ops.end(); x++) push_exec(*x);
}

void Frame::op_ticks(unsigned int param_1, unsigned int param_2) { push(Var((int)ticks)); }

void Frame::op_caller(unsigned int param_1, unsigned int param_2) { push(caller); }

void Frame::op_source(unsigned int param_1, unsigned int param_2) { push(source); }

void Frame::op_selector(unsigned int param_1, unsigned int param_2) { push(Var(selector)); }

void Frame::op_args(unsigned int param_1, unsigned int param_2) { push(List::from_vector(args)); }
void Frame::op_slice(unsigned int param_1, unsigned int param_2) {
  /** Grab index from stack
   */
  Var idx(pop());

  /** Grab value from stack
   */
  Var val(pop());

  push(val.lookup(idx));
}

void Frame::op_getrange(unsigned int param_1, unsigned int param_2) {
  /** Grab length from stack
   */
  Var eng(pop());

  /** Grab begin from stack
   */
  Var idx(pop());

  /** Grab value from stack
   */
  Var val(pop());

  push(val.subseq(idx.toint(), eng.toint()));
}

void Frame::op_setvar(unsigned int param_1, unsigned int param_2) {
  /** Grab new  value from stack
   */
  Var val(pop());

  /** assign it
   */
  scope.set(param_1, val);

  push(val);
}

void Frame::op_getvar(unsigned int param_1, unsigned int param_2) {
  /** return value
   */
  push(scope.get(param_1));
}

void Frame::op_getprivate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the slot.  Accessor == current frame's definer.
   */
  push(Slots::get_slot(self, definer, slot_sym).value);
}

void Frame::op_declprivate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  push(Var(self.declare(definer, slot_sym, val)));
}

void Frame::op_setprivate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("method selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  /** Push the slot object
   */
  Var slot = self.assign(definer, slot_sym, val);

  /** Push the slot
   */
  push(Var(slot));

  /** Push the arguments to slot.perform() -- None
   */
  push(Var());
}

void Frame::op_slots(unsigned int param_1, unsigned int param_2) { push(self.slots()); }

void Frame::op_rmprivate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in slot removal");

  Symbol slot_sym(sym.as_symbol());

  /** Remove it.
   */
  self.remove(definer, slot_sym);

  push(Var());
}

void Frame::op_getverb(unsigned int param_1, unsigned int param_2) {
  /** Grab the arg mask
   */
  Var argm(pop());

  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the method from the verb.
   */
  Var slotV(Slots::get_verb(self, slot_sym, argm.flatten()).value);

  push(slotV);
}

void Frame::op_declverb(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the arg mask
   */
  Var argm(pop());

  /** Grab value from stack
   */
  Var val(pop());

  if (!val.isBlock())
    throw invalid_type("method value must be a block");

  push(Slots::declare_verb(self, slot_sym, argm.flatten(), val));
}

void Frame::op_setverb(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("method selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the arg mask
   */
  Var argm(pop());

  /** Grab value from stack
   */
  Var val(pop());

  if (!val.isBlock())
    throw invalid_type("method value must be a block");

  push(Slots::assign_verb(self, slot_sym, argm.flatten(), val));
}

void Frame::op_rmverb(unsigned int param_1, unsigned int param_2) {
  /** Grab the arg mask
   */
  Var argm(pop());

  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in method removal");

  Symbol slot_sym(sym.as_symbol());

  /** Remove it.
   */
  Slots::remove_verb(self, slot_sym, argm.flatten());

  push(NONE);
}

void Frame::op_getdelegate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the method.  Accessor == #method.
   */
  Var slotV(Slots::get_delegate(self, slot_sym).value);

  push(slotV);
}

void Frame::op_decldelegate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  push(Var(self.declare(Var(DELEGATE_SYM), slot_sym, val)));
}

void Frame::op_setdelegate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("method selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  push(Var(self.assign(Var(DELEGATE_SYM), slot_sym, val)));
}

void Frame::op_rmdelegate(unsigned int param_1, unsigned int param_2) {
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in method removal");

  Symbol slot_sym(sym.as_symbol());

  /** Remove it.
   */
  self.remove(Var(DELEGATE_SYM), slot_sym);

  push(Var());
}

void Frame::op_declname(unsigned int param_1, unsigned int param_2) {
  /** Grab name from stack
   */
  Var name(pop());

  /** Grab object from stack
   */
  Var object(pop());

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for publishing must be a symbol");

  Symbol name_sym(name.as_symbol());

  /** Insert the name slot
   */
  try {
    self.declare(Var(NAME_SYM), name_sym, object);
  } catch (const Ref<Error> &err) {
    self.assign(Var(NAME_SYM), name_sym, object);
  }

  push(object);
}

void Frame::op_rmname(unsigned int param_1, unsigned int param_2) {
  Var name(pop());

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for unpublishing must be a symbol");

  Symbol name_sym(name.as_symbol());

  self.remove(Var(NAME_SYM), name_sym);

  push(NONE);
}

void Frame::op_setname(unsigned int param_1, unsigned int param_2) {
  Var name(pop());

  Var value(pop());

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for unpublishing must be a symbol");

  Symbol name_sym(name.as_symbol());

  push(self.assign(Var(NAME_SYM), name_sym, value)->value());
}

void Frame::op_getname(unsigned int param_1, unsigned int param_2) {
  Var name = pop();

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for unpublishing must be a symbol");

  Symbol name_sym(name.as_symbol());

  /** Names are in ( #name, <name> ) slots.
   */
  Var name_slot(Slots::get_name(self, name_sym).value);

  push(name_slot);
}

void Frame::op_destroy(unsigned int param_1, unsigned int param_2) {
  /** Remove the object from Names and the HandleFactory.
   *  Garbage collection should take care of the rest.
   */
  Pools::instance.remove(self);

  push(Var());
}

void Frame::op_suspend(unsigned int param_1, unsigned int param_2) { ex_state = SUSPENDED; }

void Frame::op_send(unsigned int param_1, unsigned int param_2) {
  /** Grab destination from stack
   */
  Var dest(pop());

  /** Grab selector from stack
   */
  Var sel(pop());

  if (sel.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in message send");

  Symbol selector_sym(sel.as_symbol());

  /** Grab arguments from stack
   */
  Var args(pop());

  /** Queue the message send and block on it.
   */
  Var msg = send(source, self, dest, dest, selector_sym, args->asType<List *>()->as_vector());

  push(msg);
  push(List::empty());
}

void Frame::op_send_like(unsigned int param_1, unsigned int param_2) {
  /** Grab destination from stack
   */
  Var dest(pop());

  /** Grab selector from stack
   */
  Var sel(pop());

  if (sel.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in message send");

  Symbol selector_sym(sel.as_symbol());

  /** Grab arguments from stack
   */
  Var args(pop());

  /** Grab "like" from stack
   */
  Var like(pop());

  /** If dest !isA like, we have a problem
   */
  if (!Slots::isA(dest, like))
    throw invalid_type("qualifier in qualified send is not an ancestor of message destination");

  /** Queue the message send and block on it.
   */
  Var msg = send(source, self, dest, like, selector_sym, args->asType<List *>()->as_vector());

  push(msg);
  push(List::empty());
}

void Frame::op_pass(unsigned int param_1, unsigned int param_2) {
  /** Pop destination
   */
  Var destination(pop());

  /** Pop arguments
   */
  Var arguments(pop());

  /** If args is None then assume current arguments.
   */
  if (!arguments) {
    arguments = List::from_vector(args);
  }

  /** Get the delegates list
   */
  var_vector delegates(self.delegates());

  /** If destination is None then get first parent.  If it's not None,
   *  verify that the specified parent is really an immediate parent of
   *  the object in question.
   */
  if (!destination) {
    /** If no destination was specified, we delegate to the first
     *  ancestor only
     */
    if (!delegates.empty())
      destination = delegates[0];
    else
      throw slot_not_found("object has no delegates");
  } else {
    var_vector::iterator fi = ::find(delegates.begin(), delegates.end(), destination);

    if (fi == delegates.end())
      throw permission_error("parent not found in object's immediate delegates list");
  }

  /** This is now just a message send, with some special arguments.
   */
  push(send(source, caller, self, destination, selector, arguments->asType<List *>()->as_vector()));
}

void Frame::op_notify(unsigned int param_1, unsigned int param_2) {
  Var arg(pop());

  push(Scheduler::instance->notify(self, arg));
}

void Frame::op_detach(unsigned int param_1, unsigned int param_2) {
  Scheduler::instance->detach(self);
  push(Var());
}

void Frame::op_scatter(unsigned int param_1, unsigned int param_2) {
  unsigned int i;

  /** Number of required Vars
   */
  unsigned required_vars = param_1;

  /** Number of optional vars = param_2 >> 1
   */
  unsigned optional_vars = param_2 >> 1;

  /** Remainder flag = param_2 & 0x01
   */
  bool has_remainders = param_2 & 0x01;

  /** Pop the range
   */
  Var range(pop());

  /** Position in the range
   */
  unsigned int pos = 0;

  var_vector arg_v(range.flatten());
  unsigned int arg_length = arg_v.size();
  if (required_vars > arg_length)
    throw arguments_err("insufficient arguments");

  for (i = 0; i < required_vars; i++) {
    /** Now set each of them
     */
    unsigned int var = next().toint();

    /** assign it
     */
    scope.set(var, arg_v[pos]);

    pos++;
  }

  for (i = 0; i < optional_vars; i++) {
    /** Now set each of them
     */
    unsigned int var = (unsigned int)next().toint();

    /** If there's nothing more in the range,
     *  we're finished.
     */
    if (pos == arg_length)
      break;
    else {
      /** assign it
       */
      scope.set(var, arg_v[pos]);

      pos++;
    }
  }

  /** Get the remainder variable if any
   */
  if (has_remainders) {
    int var = next().toint();

    /** If there's anything to give, give it.  Otherwise leave as is.
     */
    if (pos < arg_length) {
      var_vector remain(arg_v.begin() + pos, arg_v.end());
      Var ret_rem = List::from_vector(remain);
      scope.set(var, ret_rem);
    }
  } else if (pos != arg_length)
    throw arguments_err("too many arguments");

  push(range);
}

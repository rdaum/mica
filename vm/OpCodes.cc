/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <iostream>

#include <vector>
#include <utility>
#include <assert.h>


#include "Data.hh"
#include "Var.hh"
#include "Scalar.hh"
#include "List.hh"
#include "Map.hh"
#include "Set.hh"

#include "Closure.hh"
#include "Block.hh"
#include "Symbol.hh"
#include "Error.hh"

#include "Closure.hh"
#include "Scheduler.hh"
#include "Object.hh"
#include "Pool.hh"
#include "Message.hh"
#include "Pools.hh"
#include "GlobalSymbols.hh"
#include "NoReturn.hh"
#include "Exceptions.hh"
#include "Slots.hh"
#include "OpCodes.hh"

using namespace std;
using namespace mica;

OpInfo opcode_info[] = {
  { Op::LIST_MARKER,"LIST_MARKER",  &Closure::op_fail, 0 },
  { Op::SET_MARKER, "SET_MARKER",   &Closure::op_fail, 0 },
  { Op::MAP_MARKER, "MAP_MARKER",   &Closure::op_fail, 0 },
  { Op::POP_LIST,	"POP_LIST",	&Closure::op_pop_list, 0 },
  { Op::POP_SET,	"POP_SET",	&Closure::op_pop_set, 0 },
  { Op::POP_MAP, 	"POP_MAP",	&Closure::op_pop_map, 0 },
  { Op::POP,	"POP",		&Closure::op_pop, 0},
  { Op::FLATTEN,	"FLATTEN",	&Closure::op_flatten, 0},

  { Op::SLICE,      "SLICE",        &Closure::op_slice, 0},
  { Op::GETRANGE,   "GETRANGE",     &Closure::op_getrange, 0},

  { Op::SETVAR,	"SETVAR",	&Closure::op_setvar, 1 },
  { Op::GETVAR,	"GETVAR",	&Closure::op_getvar, 1 },

  { Op::SCATTER,	"SCATTER",	&Closure::op_scatter, 1 },

  { Op::SLOTS,	"SLOTS",	&Closure::op_slots, 0 },

  { Op::GETPRIVATE,	"GETPRIVATE",	&Closure::op_getprivate, 0 },
  { Op::DECLPRIVATE,"DECLPRIVATE",	&Closure::op_declprivate, 0 },	
  { Op::SETPRIVATE,	"SETPRIVATE",	&Closure::op_setprivate, 0 },
  { Op::RMPRIVATE,	"RMPRIVATE",	&Closure::op_rmprivate, 0 },

  { Op::GETVERB,	"GETVERB",	&Closure::op_getverb, 0 },
  { Op::DECLVERB,	"DECLVERB",	&Closure::op_declverb, 0 },	
  { Op::SETVERB,	"SETVERB",	&Closure::op_setverb, 0 },
  { Op::RMVERB,	"RMVERB",	&Closure::op_rmverb, 0 },

  { Op::DECLNAME,	"DECLNAME",	&Closure::op_declname, 1 },
  { Op::RMNAME,	"RMNAME",	&Closure::op_rmname, 0 },
  { Op::SETNAME,	"SETNAME",	&Closure::op_setname, 0 },
  { Op::GETNAME,	"GETNAME",	&Closure::op_getname, 1 },

  { Op::GETDELEGATE,"GETDELEGATE",	&Closure::op_getdelegate, 0 },
  { Op::DECLDELEGATE,"DECLDELEGATE",&Closure::op_decldelegate, 0 },	
  { Op::SETDELEGATE,"SETDELEGATE",	&Closure::op_setdelegate, 0 },
  { Op::RMDELEGATE,	"RMDELEGATE",	&Closure::op_rmdelegate, 0 },

  { Op::MAKE_OBJECT,"MAKE_OBJECT",	&Closure::op_make_object, 1 },
  { Op::DESTROY,	"DESTROY",	&Closure::op_destroy, 0 },
  { Op::SEND,	"SEND",		&Closure::op_send, 0 },
  { Op::SEND_LIKE,	"SEND_LIKE",	&Closure::op_send_like, 0 },
  { Op::PERFORM,	"PERFORM",	&Closure::op_perform, 0 },
  { Op::NOTIFY,	"NOTIFY",	&Closure::op_notify, 0 },
  { Op::DETACH,	"DETACH",	&Closure::op_detach, 0 },
  { Op::PASS,	"PASS",		&Closure::op_pass, 0 },
  { Op::RETURN,	"RETURN",	&Closure::op_return, 0 },
  { Op::TICKS,	"TICKS",	&Closure::op_ticks, 0 },
  { Op::CALLER,	"CALLER",	&Closure::op_caller, 0 },
  { Op::SELF,	"SELF",		&Closure::op_self, 0 },
  { Op::SOURCE,	"SOURCE",	&Closure::op_source, 0 },
  { Op::SELECTOR,	"SELECTOR",	&Closure::op_selector, 0 },
  { Op::ARGS,	"ARGS",		&Closure::op_args, 0 },
  { Op::NOT,	"NOT",		&Closure::op_not, 0 },
  { Op::NEG,	"NEG",		&Closure::op_neg, 0 },
  //   { Op::POS,	"POS",		&Closure::op_pos, 0 },
  //   { Op::ABS,	"ABS",		&Closure::op_abs, 0 },
  { Op::LSHIFT,	"LSHIFT",	&Closure::op_lshift, 0 },
  { Op::RSHIFT,	"RSHIFT",	&Closure::op_rshift, 0 },
  { Op::BAND,	"BAND",		&Closure::op_band, 0 },
  { Op::BOR,	"BOR",		&Closure::op_bor, 0 },
  { Op::XOR,	"XOR",		&Closure::op_xor, 0 },
  { Op::AND,	"AND",		&Closure::op_and, 0 },   
  { Op::OR,		"OR",		&Closure::op_or, 0 },
  { Op::ADD,	"ADD",		&Closure::op_add, 0 },
  { Op::SUB,	"SUB",		&Closure::op_sub, 0 },
  { Op::MUL,	"MUL",		&Closure::op_mul, 0 },
  { Op::DIV,	"DIV",		&Closure::op_div, 0 },
  { Op::MOD,	"MOD",		&Closure::op_mod, 0 },
  { Op::ISA,	"ISA",		&Closure::op_isa, 0 },
  { Op::EQUAL,	"EQUAL",	&Closure::op_equal, 0 },
  { Op::NEQUAL,	"NEQAL",	&Closure::op_nequal, 0 },
  { Op::LESST,	"LESST",        &Closure::op_lesst, 0 },
  { Op::LESSTE,	"LESSTE",       &Closure::op_lesste, 0 },
  { Op::GREATERT,  	"GREATERT",     &Closure::op_greatert, 0 },
  { Op::GREATERTE,	"GREATERTE",    &Closure::op_greaterte, 0 },

  /** Manual PC move
   */
  { Op::JMP,       	"JMP",          &Closure::op_jmp, 1 },

  /** Block begin/end
   */
  { Op::BBEGIN,	"BBEGIN",	&Closure::op_bbegin, 2 },
  { Op::BEND,	"BEND",		&Closure::op_bend, 0 },

  /** Closure creation, block evaluation
   */
  { Op::CLOSURE,	"CLOSURE",	&Closure::op_closure, 0 },
  { Op::MAKE_LAMBDA,"MAKE_LAMBDA",  &Closure::op_make_lambda, 0 },
  { Op::EVAL,	"EVAL",		&Closure::op_eval, 0 },
  //   { Op::SUSPEND,	"SUSPEND",	&Closure::op_suspend, 0 },

  /** Looping constructs
   */
  { Op::START_LOOP, "START_LOOP",   &Closure::op_start_loop, 0 },
  { Op::BREAK,      "BREAK",        &Closure::op_break, 0 },
  { Op::CONTINUE,	"CONTINUE",	&Closure::op_continue, 0 },
  { Op::FOR_RANGE, 	"FOR_RANGE",    &Closure::op_for_range, 2 },
  { Op::MAP, 	"MAP",          &Closure::op_map, 0 },

  { Op::WHILE,      "WHILE",        &Closure::op_while, 1 },

  /** Exceptions
   */
  { Op::THROW,	"THROW",	&Closure::op_throw, 0 },
  { Op::CATCH,	"CATCH",	&Closure::op_catch, 0 },

  /** IF[/ELSE]
   */
  { Op::IFELSE,	"IFELSE",	&Closure::op_ifelse, 1 },


};

vector<OpInfo*> mica::opcodes;


void mica::initializeOpcodes()
{
  size_t i;

  opcodes.resize( LAST_TOKEN + 1 );
  for (i = 0; i < (sizeof(opcode_info) / sizeof(OpInfo)); i++) {
    if (opcode_info[i].code >= 0)
      opcodes[opcode_info[i].code] = &opcode_info[i];
  }
}

void Closure::op_fail( unsigned int param_1, unsigned int param_2 )
{
  // Should never get here.

  assert(0);
}

void Closure::op_neg( unsigned int param_1, unsigned int param_2 )
{
  Var val(pop());

  push( (-val) );
}

void Closure::op_add( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left + right );
}

void Closure::op_sub( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left - right );
}

void Closure::op_mul( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left * right );
}

void Closure::op_div( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left / right );
}

void Closure::op_mod( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left % right );
}

void Closure::op_isa( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());

  push( Var( Slots::isA( left, right) ) );
}

void Closure::op_equal( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( Var( left == right ) );
}

void Closure::op_nequal( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( Var( left != right ));
}

void Closure::op_lesst( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( Var( left < right ));
}

void Closure::op_greatert( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( Var( left > right ));
}

void Closure::op_greaterte( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( Var( left >= right ) );
}

void Closure::op_lesste( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( Var( left <= right ) );
}

void Closure::op_and( unsigned int param_1, unsigned int param_2 ) 
{
  Var left(pop());
  Var right(pop());
  
  push( left && right );
}

void Closure::op_or( unsigned int param_1, unsigned int param_2 ) 
{
  Var left(pop());
  Var right(pop());
  
  push( left || right );
}

void Closure::op_xor( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());

  push( left ^ right );
}

void Closure::op_lshift( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left << right );
}

void Closure::op_rshift( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());
  
  push( left >> right );
}
  
void Closure::op_band( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());

  push( left & right );
}

void Closure::op_bor( unsigned int param_1, unsigned int param_2 )
{
  Var left(pop());
  Var right(pop());

  push( left | right );
}

void Closure::op_not( unsigned int param_1, unsigned int param_2 ) 
{
  Var val(pop());
  
  push( Var( !val ) );
}

void Closure::op_return( unsigned int param_1, unsigned int param_2 )
{
  reply_return( pop() );

  terminate();

  ex_state = STOPPED; 
}

void Closure::op_eval( unsigned int param_1, unsigned int param_2 )
{
  /** Pop the block from the stack
   */
  Var block(pop());

  /** Verify that it's a block
   */
  if (!block.isBlock())
    throw invalid_type("attempt to evaluate non-block");

  Ref<Block> block_obj = block->asRef<Block>();

  /** Now push all its instructions into the exec stack...
   */
  for (var_vector::reverse_iterator x = block_obj->code.rbegin();
       x != block_obj->code.rend(); x++)
    push_exec(*x);
    
}

void Closure::op_closure( unsigned int param_1, unsigned int param_2 )
{
  /** Quite simple.
   */
  push(Var(this));
}

void Closure::op_make_lambda( unsigned int param_1, unsigned int param_2 )
{
  /** Pop the block
   */
  Var block(pop());

  /** Verify it is a block
   */
  if (!block.isBlock())
    throw invalid_type("attempt to create a lambda from a non-block");

  Ref<Block> block_obj = block->asRef<Block>();

  /** Now create a closure for it -- copy this one to do it.
   */
  Ref<Closure> new_closure( new (aligned) Closure(this) );
  
  /** Set its block object to the new block
   */
  new_closure->set_block( block_obj );

  /** Clear its parent_task
   */
  new_closure->parent_task = 0;

  push( Var(new_closure) );
}

void Closure::op_make_object( unsigned int param_1, unsigned int param_2 )
{
  /** Pop the constructor block
   */
  Ref<Block> c_block( pop()->asRef<Block>() );
  
  /** Create a closure for it
   */
  Ref<Closure> new_closure( new (aligned) Closure(this) );
  new_closure->set_block( c_block );
  
  /** Expand its variable scope by 1, to make room for "creator"
   */
  new_closure->scope.grow( 1 );

  /** Set that variable to current self
   */
  new_closure->scope.set( param_1, self );

  /** Now create an object and put it on self + definer on the new closure
   */
  Var new_object( Object::create() );
  new_closure->self = new_object;
  new_closure->definer = new_object;

  /** Clear its parent_task
   */
  new_closure->parent_task = 0;

  push( Var(new_closure) );
}

void Closure::op_bbegin( unsigned int param_1, unsigned int param_2 )
{
  /** Grab the number of local scope to add
   */
  unsigned int add_scope = next().toint();

  /** Grab how many opcodes are in the block
   */
  unsigned int block_size = next().toint();

  /** Push a block context
   */
  bstck.push_back( BlockContext(add_scope, block_size) );

  /** Signal entrance notification
   */
  bstck.back().enter( this );
}

void Closure::op_bend( unsigned int param_1, unsigned int param_2 )
{
  /** Signal exit notification
   */
  bstck.back().exit( this );

  /** Pop block context
   */
  bstck.pop_back();
}

void Closure::op_catch( unsigned int param_1, unsigned int param_2 )
{
  /** Pop the error we're catching from the stack
   */
  Var err = pop();

  /** Grab the identifier to assign into.
   */
  int var = next().toint();

  /** Grab the size of the block so we know how to jump past it at 
   *  this time.
   */
  int skip_size = next().toint();

  /** Push a handler for it into the array for this error, with the
   *  PC at the start of the upcoming block
   */
  bstck.back().add_error_catch( err->asRef<Error>()->err_sym, pc(), var );

  /** Continue life... BEND will pop off the handler off if it's never
   *  hit.
   */
  jump( skip_size );
}



void Closure::op_perform( unsigned int param_1, unsigned int param_2 )
{
  Var args(pop()), lhs(pop());

  Var result(lhs.perform( this, args ));

  /** If it's NoReturn, don't push the result to the stack,
   *  instead, block... 
   */
  if ( result == NoReturn::instance )
    block();
  else {
    push( result ); 
  } 

}



void
Closure::op_self( unsigned int param_1, unsigned int param_2 )
{
  push(Var(self));
}

void
Closure::op_jmp( unsigned int param_1, unsigned int param_2 )
{
  /** grab PC offset
   */
  int skip = next().toint();

  /** move PC
   */
  jump(skip);
}

void Closure::op_start_loop( unsigned int param_1, unsigned int param_2 )
{
  start_loop( param_1 );
}

void Closure::op_continue( unsigned int param_1, unsigned int param_2 )
{
  do_continue();
}

void Closure::op_break( unsigned int param_1, unsigned int param_2 )
{
  do_break();
}

void Closure::op_while( unsigned int param_1, unsigned int param_2 )
{
  /** Pop the truth expr
   */
  Var truth(pop());

  /** If not true, do_break
   */
  if (!truth.truth())
    do_break();

  /** Otherwise continue on
   */
}

void Closure::op_for_range( unsigned int param_1, unsigned int param_2 )
{
 /** pop the block
   */
  Var block( pop() );

  /** pop the sequence
   */
  Var sequence( pop() );

  /** Build the instructions for iteration..
   */
  var_vector ops( sequence.for_in( param_1, block ) );

  /** Schedule them for execution.
   */
  for (var_vector::reverse_iterator x = ops.rbegin();
       x != ops.rend(); x++) {
    push_exec( *x );
  }

}
 
void Closure::op_map( unsigned int param_1, unsigned int param_2 )
{
  /** pop the expr
   */
  Var expr( pop() );

  /** pop the sequence
   */
  Var sequence( pop() );

  /** Build the instructions for iteration..
   */
  var_vector ops( sequence.map( expr ) );

  /** Schedule them for execution.
   */
  for (var_vector::iterator x = ops.begin();
       x != ops.end(); x++) {
    push_exec( *x );
  }

}

void
Closure::op_ifelse( unsigned int param_1, unsigned int param_2 )
{
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
  if (tt.truth()) {
   
    var_vector ops( success->asType<List*>()->as_vector() );
    for (var_vector::reverse_iterator x = ops.rbegin();
	 x != ops.rend(); x++) 
      push_exec(*x);
    
  } else {

    var_vector ops( fail->asType<List*>()->as_vector() );
    for (var_vector::reverse_iterator x = ops.rbegin();
	 x != ops.rend(); x++) 
      push_exec(*x);
  }


}

void
Closure::op_throw( unsigned int param_1, unsigned int param_2 )
{
  /** Pop error
   */
  Var err(pop());

  if (err.type_identifier() != Type::ERROR)
    throw invalid_type("attempt to throw non-error type");

  /** Raise it
   */
  raise( err->asRef<Error>() );

}

void Closure::op_pop_list( unsigned int param_1, unsigned int param_2 )
{
  var_vector lst;

  Var element;

  while ( (element = pop()).type_identifier() != Type::OPCODE ) {
    lst.push_back(element);
  }
  
  if (element != Var(Op::LIST_MARKER))
    throw internal_error("stack underflow in pop_list");

  push( List::from_vector(lst) );
  
}


void Closure::op_pop_set( unsigned int param_1, unsigned int param_2 )
{
  var_set new_set;

  Var element;

  while ( (element = pop()).type_identifier() != Type::OPCODE ) {
    new_set.insert( element ); 
  }
  
  if (element != Var(Op::SET_MARKER))
    throw internal_error("stack underflow in pop_set");

  push( Set::from_set( new_set ) );
  
}

void Closure::op_pop_map( unsigned int param_1, unsigned int param_2 )
{
  Var left, right;
  var_map x;


  while ( (right = pop()).type_identifier() != Type::OPCODE ) {
    Var left(pop());
    x[left] = right;
  }

 
  if (right != Var(Op::MAP_MARKER))
    throw internal_error("stack underflow in pop_map");

  push( Map::from_map(x) );
}

void Closure::op_pop( unsigned int param_1, unsigned int param_2 )
{
  pop();
}

void Closure::op_flatten( unsigned int param_1, unsigned int param_2 )
{
  Var x = pop();

  var_vector ops( x.flatten() );
  for (var_vector::iterator x = ops.begin(); x != ops.end(); x++) 
    push_exec(*x);

}


void Closure::op_ticks( unsigned int param_1, unsigned int param_2 )
{
  push( Var( (int)ticks ) );
}

void Closure::op_caller( unsigned int param_1, unsigned int param_2 )
{
  push( caller );
}

void Closure::op_source( unsigned int param_1, unsigned int param_2 )
{
  push( source );
}

void Closure::op_selector( unsigned int param_1, unsigned int param_2 )
{
  push( Var(selector) );
}

void Closure::op_args( unsigned int param_1, unsigned int param_2 )
{
  push( List::from_vector( args ) );
}
void Closure::op_slice( unsigned int param_1, unsigned int param_2 )
{
  /** Grab index from stack
   */
  Var idx(pop());

  /** Grab value from stack
   */
  Var val(pop());

  push( val.lookup(idx) );
}

void Closure::op_getrange( unsigned int param_1, unsigned int param_2 )
{
  /** Grab length from stack
   */
  Var eng(pop());

  /** Grab begin from stack
   */
  Var idx(pop());

  /** Grab value from stack
   */
  Var val(pop());

  push( val.subseq(idx.toint(), eng.toint()) );
}

void Closure::op_setvar( unsigned int param_1, unsigned int param_2 )
{
  /** Grab new  value from stack
   */
  Var val(pop());

  /** assign it
   */
  scope.set( param_1, val );

  push( val );
}

void Closure::op_getvar( unsigned int param_1, unsigned int param_2 )
{
  /** return value
   */
  push( scope.get( param_1 ) );
}

void Closure::op_getprivate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the slot.  Accessor == current closure's definer.
   */
  push( Slots::get_slot( self, definer, slot_sym ).value );
}

void Closure::op_declprivate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  push( Var(self.declare( definer, slot_sym, val ) ) );
}


void Closure::op_setprivate( unsigned int param_1, unsigned int param_2 )
{
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
  Var slot = self.assign( definer, slot_sym, val );

  /** Push the slot
   */
  push( Var(slot) );

  /** Push the arguments to slot.perform() -- None
   */
  push( Var() );
}

void Closure::op_slots( unsigned int param_1, unsigned int param_2 )
{
  push( self.slots() );
}


void Closure::op_rmprivate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in slot removal");

  Symbol slot_sym(sym.as_symbol());

  /** Remove it.
   */
  self.remove( definer, slot_sym );

  push(Var());
}

void Closure::op_getverb( unsigned int param_1, unsigned int param_2 )
{
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
  Var slotV( Slots::get_verb( self, slot_sym, argm.flatten() ).value);
  
  push( slotV );
}


void Closure::op_declverb( unsigned int param_1, unsigned int param_2 )
{
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

  push( Slots::declare_verb( self, slot_sym, argm.flatten(), val ) );
}

void Closure::op_setverb( unsigned int param_1, unsigned int param_2 )
{
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

  push( Slots::assign_verb( self, slot_sym, argm.flatten(), val ) );

}



void Closure::op_rmverb( unsigned int param_1, unsigned int param_2 )
{
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
  Slots::remove_verb( self, slot_sym, argm.flatten() );

  push(NONE);
}

void Closure::op_getdelegate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab the method.  Accessor == #method.
   */
  Var slotV( Slots::get_delegate( self, slot_sym ).value);
  
  push( slotV );
}


void Closure::op_decldelegate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("slot selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  push( Var(self.declare( Var(DELEGATE_SYM), slot_sym, val )) );
}

void Closure::op_setdelegate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("method selector must be a symbol");

  Symbol slot_sym(sym.as_symbol());

  /** Grab value from stack
   */
  Var val(pop());

  push( Var(self.assign( Var(DELEGATE_SYM), slot_sym, val ) ));
}



void Closure::op_rmdelegate( unsigned int param_1, unsigned int param_2 )
{
  /** Grab symbol from stack
   */
  Var sym(pop());

  if (sym.type_identifier() != Type::SYMBOL)
    throw invalid_type("invalid selector in method removal");

  Symbol slot_sym(sym.as_symbol());

  /** Remove it.
   */
  self.remove( Var(DELEGATE_SYM), slot_sym );

  push(Var());
}

void Closure::op_declname( unsigned int param_1, unsigned int param_2 )
{
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

  Symbol name_sym( name.as_symbol() );

  /** Insert the name slot
   */
  try {
    self.declare( Var(NAME_SYM), name_sym, object );
  } catch (const Ref<Error> &err) {
    self.assign( Var(NAME_SYM), name_sym, object );
  }

  push(object);
}

void Closure::op_rmname( unsigned int param_1, unsigned int param_2 )
{
  Var name(pop());

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for unpublishing must be a symbol");

  Symbol name_sym( name.as_symbol() );

  self.remove( Var(NAME_SYM), name_sym );

  push(NONE);
}

void Closure::op_setname( unsigned int param_1, unsigned int param_2 )
{
  Var name(pop());
  
  Var value(pop());

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for unpublishing must be a symbol");

  Symbol name_sym( name.as_symbol() );

  push( self.assign( Var(NAME_SYM), name_sym, value )->value() );
}


void Closure::op_getname( unsigned int param_1, unsigned int param_2 )
{
  Var name = pop();

  /** Make sure the name is a symbol
   */
  if (name.type_identifier() != Type::SYMBOL)
    throw invalid_type("name for unpublishing must be a symbol");

  Symbol name_sym( name.as_symbol() );

  /** Names are in ( #name, <name> ) slots.
   */
  Var name_slot( Slots::get_name( self, name_sym ).value );
  
  push( name_slot );
}


void Closure::op_destroy( unsigned int param_1, unsigned int param_2 )
{
  /** Remove the object from Names and the HandleFactory.  
   *  Garbage collection should take care of the rest.
   */
  Pools::instance.remove( self );

  push(Var());
}



void Closure::op_send( unsigned int param_1, unsigned int param_2 )
{
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
  Var msg = send( source, self, dest, dest,
		  selector_sym, args->asType<List*>()->as_vector() );
  
  push( msg );
  push( List::empty()
 );
}

void Closure::op_send_like( unsigned int param_1, unsigned int param_2 )
{

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
  if (!Slots::isA( dest, like))
    throw invalid_type("qualifier in qualified send is not an ancestor of message destination");

  /** Queue the message send and block on it.
   */
  Var msg = send( source, self, dest, like,
		  selector_sym, args->asType<List*>()->as_vector() );
  
  push( msg );
  push( List::empty()
 );
}
 
void Closure::op_pass( unsigned int param_1, unsigned int param_2 ) 
{
  /** Pop destination
   */
  Var destination(pop());

  /** Pop arguments
   */
  Var arguments(pop());

  /** If args is None then assume current arguments.
   */
  if (!arguments) {
    arguments = List::from_vector
( args );
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
    var_vector::iterator fi = ::find( delegates.begin(),
				      delegates.end(),
				      destination );

    if (fi == delegates.end())
      throw permission_error("parent not found in object's immediate delegates list");
  }

  /** This is now just a message send, with some special arguments.
   */
  push( send( source, caller, self, destination, selector, 
	      arguments->asType<List*>()->as_vector() ) );


}  


void Closure::op_notify( unsigned int param_1, unsigned int param_2 )
{
  Var arg(pop());

  push( Scheduler::instance->notify( self, arg ) );
}

void Closure::op_detach( unsigned int param_1, unsigned int param_2 )
{
  Scheduler::instance->detach( self );
  push( Var() );
}

void Closure::op_scatter( unsigned int param_1, unsigned int param_2 )
{
  unsigned int i;

  /** Pop the range
   */
  Var range(pop());

  /** Position in the range
   */
  unsigned int pos = 0;

  /** Retrieve # of required scope
   */
  unsigned int nscope = next().toint();

  var_vector arg_v( range.flatten() );
  unsigned int arg_length = arg_v.size();
  if (nscope > arg_length)
    throw arguments_err("insufficient arguments");

  for (i = 0; i < nscope; i++) {
    /** Now set each of them
     */
    unsigned int var = next().toint();

    /** assign it
     */
    scope.set( var, arg_v[pos] ) ;

    pos++;
  }

  /** Retrieve # of opt scope
   */
  nscope = next().toint();

  for (i = 0; i < nscope; i++) {
    /** Now set each of them
     */
    unsigned int var = (unsigned int) next().toint();

    /** If there's nothing more in the range,
     *  we're finished.
     */
    if (pos == arg_length)
      break;
    else {
      /** assign it
       */
      scope.set( var, arg_v[pos] );
      
      pos++;
    } 
  }

  /** Get the remainder variable if any
   */
  Var remainder_var = next();
  if (remainder_var != Var(false)) {
    int var = next().toint();
    
    /** If there's anything to give, give it.  Otherwise leave as is.
     */
    if (pos < arg_length ) {
      var_vector remain( arg_v.begin() + pos, arg_v.end() );
      Var ret_rem = List::from_vector( remain );
      scope.set( var, ret_rem );
    }
  } else
    if (pos != arg_length)
      throw arguments_err("too many arguments");
  
  push( range );
}

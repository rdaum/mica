/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef OPCODE_HH
#define OPCODE_HH

#include "Atoms.hh"

namespace mica {

  struct Op {
    /** Here are the OpCode entries for the virtual machine
     */
    typedef enum {

      // Special marker codes.
      LIST_MARKER = -3, SET_MARKER, MAP_MARKER, 

      // Stack manipulation
      POP_LIST, POP_MAP,  POP_SET, POP, FLATTEN,

      // Local variable manipulation
      SETVAR, GETVAR, SCATTER,

      // Private slot manipulation
      GETPRIVATE, DECLPRIVATE, SETPRIVATE, SLOTS, RMPRIVATE, 

      // Verb slot manipulation
      GETVERB, DECLVERB, SETVERB, RMVERB, 

      // Name slot manipulation
      DECLNAME, RMNAME, GETNAME, SETNAME,

      // Delegate slot manipulation
      DECLDELEGATE, RMDELEGATE, GETDELEGATE, SETDELEGATE,

      // Object manipulation
      MAKE_OBJECT, DESTROY, COMPOSE,

      // Messaging
      SEND, SEND_LIKE, PASS, PASS_TO, RETURN, SUSPEND, PERFORM, 
    
      // NOTIFICATION
      NOTIFY, DETACH,
    
      // Runtime status/information
      TICKS, SELF, CALLER, SOURCE, SELECTOR, ARGS, 

      // Unary ops
      NOT, NEG, 

      // Binary ops
      LSHIFT, RSHIFT, SLICE, AND, XOR, OR, ADD, SUB, MUL, DIV, MOD, 
      EQUAL, NEQUAL, LESST, GREATERT, LESSTE, GREATERTE, BAND, BOR,
      ISA, CDR, CAR, CONS, 

      // More-than-binary ops,
      GETRANGE,

      // Make current closure as value
      CLOSURE,

      // Replace current closure with another.
      J,

      // Evaluate an expression on the stack in a sub-branch
      EVAL,

      // As above, but replace current branch
      TRAMPOLINE,

      // Restore from the branch stack
      JOIN,

      // Make a lambda expression
      MAKE_LAMBDA, 

      // Iteration
      MAP, 

      // Looping
      BREAK, CONTINUE, LOOP,

      // Error
      CATCH, THROW,

      // If/else
      IF, IFELSE

    } Code;

    bool         is_integer : 1;
    bool         is_pointer : 1;
    Atoms::types type       : 3;
    Code         code       : 8;
    unsigned int param_1    : 10;
    unsigned int param_2    : 9;

    Op() 
      : is_integer(false), is_pointer(false), type(Atoms::OPCODE),
	code(IFELSE), param_1(0), param_2(0) {};

    Op( const Code &operation_code, unsigned int parameter_1 = 0, 
	unsigned int parameter_2 = 0 ) :
      is_integer(false), is_pointer(false), type(Atoms::OPCODE),
      code(operation_code), param_1(parameter_1), param_2(parameter_2) {};

    Op( const Op &opcode ) :
      is_integer(false), is_pointer(false), type(Atoms::OPCODE),
      code(opcode.code), param_1(opcode.param_1), param_2(opcode.param_2) {};

    Op( const _Atom &atom_conversion )
    {
      memcpy( this, &atom_conversion, sizeof( atom_conversion ) );
      assert( type == Atoms::OPCODE);
    }

    bool operator<( const Op &opcode ) const {
      return code < opcode.code;
    }

    bool operator==( const Op &opcode ) const {
      return code == opcode.code;
    }

    bool operator==( Code operation_code ) const {
      return code == operation_code;
    }

  };
}

#endif

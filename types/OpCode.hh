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
      ISA,

      // More-than-binary ops,
      GETRANGE,

      // Block context handling
      BBEGIN, BEND,

      // Get current closure
      CLOSURE,

      // Blocks, closures
      EVAL, MAKE_LAMBDA, 

      // Iteration
      FOR_RANGE, MAP, 

      // Looping
      START_LOOP, END_LOOP, BREAK, CONTINUE, WHILE,

      // PC
      JMP,

      // Error
      CATCH, THROW,

      // If/else
      IFELSE

    } Code;

    bool         is_integer : 1;
    bool         is_pointer : 1;
    Atoms::types type       : 2;
    Code         code       : 8;
    unsigned int param      : 20;

    Op() 
      : is_integer(false), is_pointer(false), type(Atoms::OPCODE),
	code(IFELSE), param(0) {};

    Op( const Code &operation_code, unsigned int parameter = 0 ) :
      is_integer(false), is_pointer(false), type(Atoms::OPCODE),
      code(operation_code), param(parameter) {};

    Op( const Op &opcode ) :
      is_integer(false), is_pointer(false), type(Atoms::OPCODE),
      code(opcode.code), param(opcode.param) {};

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

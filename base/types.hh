#ifndef MICA_TYPES_HH
#define MICA_TYPES_HH

#include "rope_string.hh"

namespace mica {

/** An enumeration of all the types available in the system.
 *  Used for purposes of comparison, serialization, and unserialization.
 */
struct Type {
  typedef enum { 
    // SCALARS (HELD IN VAR)
    INTEGER, FLOAT, CHAR, OPCODE, BOOL,
    
    // BASE TYPES
    COMPOSITE, ERROR, ITERATOR, LIST, MAP,
    OBJECT, SET, STRING, SYMBOL,
    
    // VM-PROVIDED TYPES
    BLOCK, CLOSURE, 
    MESSAGE, RETURNMESSAGE, RAISEMESSAGE, HALTMESSAGE, EXECUTABLEMESSAGE,
    NATIVEBLOCK, NATIVECLOSURE, NORETURN, TASK, TASK_HANDLE,
    
    // COMPILER-PROVIDED TYPES
    NODE, 

    // MISC UTIL TYPES
    GRAPH_VISITOR,

    // ABSTRACT
    ABSTRACT
  } Identifier;
};


  class has_type_identifier 
  {
  public:
    virtual Type::Identifier type_identifier() const = 0;
  };

  template<Type::Identifier TYPE_ID>
  class has_constant_type_identifier
    : public has_type_identifier
  {
  public:
    Type::Identifier type_identifier() const {
      return TYPE_ID;
    }
  };
 
}

#endif /** MICA_TYPES_HH **/
/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef OPCODES_HH
#define OPCODES_HH

#include "OpCode.hh"
#include <vector>

#define MAX_TOKEN_LEN 16
#define LAST_TOKEN (int)Op::IFELSE

namespace mica {

  class Closure;
 
  typedef void (Closure::*PFV)(unsigned int);
  
  struct OpInfo {
    int code;                       // opcode index
    char name[MAX_TOKEN_LEN];	    // human readable name
    PFV func;                       // pointer to the impl. function
    int nargs;                      // number of args needed on stack
  } ;
 
  extern std::vector<OpInfo*> opcodes;
 
  extern void initializeOpcodes();

} 
#endif

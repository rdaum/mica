/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef GLOBALSYMBOLS_HH
#define GLOBALSYMBOLS_HH

namespace mica {

  extern void initSymbols();

  /** Errors
   */
  extern Ref<Error> E_SLOTNF;
  extern Ref<Error> E_NOTF;
  extern Ref<Error> E_NONE;
  extern Ref<Error> E_PARSE;
  extern Ref<Error> E_PERM;
  extern Ref<Error> E_SLOTOVLD;

  /** Some commonly used symbols
   */

  extern Symbol LAMBDA_SYM;
  extern Symbol EVAL_SYM;  
  extern Symbol PERFORM_SYM; 
  extern Symbol PARENT_SYM;

  extern Symbol INITIALIZE_SYM;
  extern Symbol PARSE_SYM;
  extern Symbol EVAL_TMP_SYM;

  extern Symbol RECEIVE_SYM;
  extern Symbol ATTACH_SYM;
  extern Symbol DETACH_SYM;

  extern Symbol ARGUMENTS_SYM;
  extern Symbol OUT_OF_RANGE_SYM;
  extern Symbol ARGUMENTS_SYM;
  extern Symbol OUT_OF_RANGE_SYM;
  extern Symbol UNIMPLEMENTED_SYM;
  extern Symbol INVALID_TYPE_SYM;
  extern Symbol SLOT_NOT_FOUND_SYM;
  extern Symbol SLOT_OVERLOAD_SYM;
  extern Symbol NOT_FOUND_SYM;  
  extern Symbol STACK_UNDERFLOW_SYM;
  extern Symbol VAR_NOT_FOUND_SYM;
  extern Symbol INTERNAL_SYM;
  extern Symbol MAX_TICKS_SYM;
  extern Symbol TERMINATED_SYM;
  extern Symbol MAX_RECURSION_SYM;
  extern Symbol PARSE_SYM;
  extern Symbol BLOCKED_SYM;
  extern Symbol PERMISSION_SYM;
  extern Symbol DIVZERO_SYM;
  extern Symbol NONE_SYM;

  extern Symbol NAME_SYM;
  extern Symbol TITLE_SYM;
  extern Symbol DELEGATE_SYM;
  extern Symbol VERB_SYM;
}

#endif

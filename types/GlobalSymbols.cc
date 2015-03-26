/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "types/GlobalSymbols.hh"


#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"

using namespace mica;

Var mica::NONE;

void mica::initSymbols() {
  /** This should be the first symbol created.
   *  Its index into the symbol vector should
   *  be 0.
   */
  NONE = Var(Symbol::create("none"));

  EVAL_SYM = Symbol::create("eval");
  LAMBDA_SYM = Symbol::create("lambda");
  PERFORM_SYM = Symbol::create("perform");
  PARENT_SYM = Symbol::create("parent");

  INITIALIZE_SYM = Symbol::create("initialize");
  LAMBDA_SYM = Symbol::create("lambda");
  PARSE_SYM = Symbol::create("parse");
  RECEIVE_SYM = Symbol::create("receive");
  ATTACH_SYM = Symbol::create("connect");
  DETACH_SYM = Symbol::create("detach");

  EVAL_TMP_SYM = Symbol::create("eval_tmp");

  ARGUMENTS_SYM = Symbol::create("arguments");
  OUT_OF_RANGE_SYM = Symbol::create("out_of_range");
  ARGUMENTS_SYM = Symbol::create("arguments");
  OUT_OF_RANGE_SYM = Symbol::create("out_of_range");
  UNIMPLEMENTED_SYM = Symbol::create("unimplemented");
  INVALID_TYPE_SYM = Symbol::create("invalid_type");
  SLOT_NOT_FOUND_SYM = Symbol::create("slot_not_found");
  SLOT_OVERLOAD_SYM = Symbol::create("slot_overload");
  NOT_FOUND_SYM = Symbol::create("not_found");
  STACK_UNDERFLOW_SYM = Symbol::create("stack_underflow");
  VAR_NOT_FOUND_SYM = Symbol::create("var_not_found");
  INTERNAL_SYM = Symbol::create("internal");
  MAX_TICKS_SYM = Symbol::create("max_ticks");
  TERMINATED_SYM = Symbol::create("terminated");
  MAX_RECURSION_SYM = Symbol::create("max_recursion");
  PARSE_SYM = Symbol::create("parse");
  BLOCKED_SYM = Symbol::create("blocked");

  PERMISSION_SYM = Symbol::create("permission");
  DIVZERO_SYM = Symbol::create("divzero");
  NONE_SYM = Symbol::create("none");

  VERB_SYM = Symbol::create("METHOD");
  NAME_SYM = Symbol::create("NAME");
  TITLE_SYM = Symbol::create("title");
  DELEGATE_SYM = Symbol::create("DELEGATE");

  E_SLOTNF = slot_not_found("slot not found");
  E_NOTF = not_found("not found");
  E_NONE = no_error("no error");
  E_PARSE = parse_error("parse error");
  E_PERM = permission_error("permission denied");
  E_SLOTOVLD = slot_overload("slot already declared");
}

Ref<Error> mica::E_SLOTNF(0);
Ref<Error> mica::E_SLOTOVLD(0);
Ref<Error> mica::E_NOTF(0);
Ref<Error> mica::E_NONE(0);
Ref<Error> mica::E_PARSE(0);
Ref<Error> mica::E_PERM(0);

Symbol mica::LAMBDA_SYM;
Symbol mica::EVAL_SYM;
Symbol mica::PARENT_SYM;
Symbol mica::PERFORM_SYM;

Symbol mica::INITIALIZE_SYM;
Symbol mica::PARSE_SYM;
Symbol mica::EVAL_TMP_SYM;

Symbol mica::RECEIVE_SYM;
Symbol mica::ATTACH_SYM;
Symbol mica::DETACH_SYM;

Symbol mica::ARGUMENTS_SYM;
Symbol mica::OUT_OF_RANGE_SYM;
Symbol mica::UNIMPLEMENTED_SYM;
Symbol mica::INVALID_TYPE_SYM;
Symbol mica::SLOT_NOT_FOUND_SYM;
Symbol mica::SLOT_OVERLOAD_SYM;
Symbol mica::NOT_FOUND_SYM;
Symbol mica::STACK_UNDERFLOW_SYM;
Symbol mica::VAR_NOT_FOUND_SYM;
Symbol mica::INTERNAL_SYM;
Symbol mica::MAX_TICKS_SYM;
Symbol mica::TERMINATED_SYM;
Symbol mica::MAX_RECURSION_SYM;
Symbol mica::BLOCKED_SYM;
Symbol mica::PERMISSION_SYM;
Symbol mica::DIVZERO_SYM;
Symbol mica::NONE_SYM;

Symbol mica::TITLE_SYM;
Symbol mica::NAME_SYM;
Symbol mica::DELEGATE_SYM;
Symbol mica::VERB_SYM;
Symbol mica::CREATOR_SYM;

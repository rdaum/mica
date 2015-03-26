/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Exceptions.hh"

#include <cstdio>
#include <sstream>
#include <stdexcept>
#include <vector>


#include "types/Data.hh"
#include "types/Error.hh"
#include "types/GlobalSymbols.hh"
#include "types/String.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"

using namespace mica;

Ref<Error> mica::arguments_err(const char *whatString) {
  return new Error(ARGUMENTS_SYM, String::create(whatString));
}

Ref<Error> mica::out_of_range(const char *whatString) {
  return new Error(OUT_OF_RANGE_SYM, String::create(whatString));
}

Ref<Error> mica::unimplemented(const char *whatString) {
  return new Error(UNIMPLEMENTED_SYM, String::create(whatString));
}

Ref<Error> mica::invalid_type(const char *whatString) {
  return new Error(INVALID_TYPE_SYM, String::create(whatString));
}

Ref<Error> mica::slot_not_found(const char *whatString) {
  return new Error(SLOT_NOT_FOUND_SYM, String::create(whatString));
}

Ref<Error> mica::slot_overload(const char *whatString) {
  return new Error(SLOT_OVERLOAD_SYM, String::create(whatString));
}

Ref<Error> mica::not_found(const char *whatString) {
  return new Error(NOT_FOUND_SYM, String::create(whatString));
}

Ref<Error> mica::stack_underflow(const char *whatString) {
  return new Error(STACK_UNDERFLOW_SYM, String::create(whatString));
}

Ref<Error> mica::var_not_found(const char *whatString) {
  return new Error(VAR_NOT_FOUND_SYM, String::create(whatString));
}

Ref<Error> mica::internal_error(const char *whatString) {
  return new Error(INTERNAL_SYM, String::create(whatString));
}

Ref<Error> mica::max_ticks(const char *whatString) {
  return new Error(MAX_TICKS_SYM, String::create(whatString));
}

Ref<Error> mica::terminated(const char *whatString) {
  return new Error(TERMINATED_SYM, String::create(whatString));
}

Ref<Error> mica::max_recursion(const char *whatString) {
  return new Error(MAX_RECURSION_SYM, String::create(whatString));
}

Ref<Error> mica::parse_error(const char *whatString) {
  return new Error(PARSE_SYM, String::create(whatString));
}

Ref<Error> mica::block_error(const char *whatString) {
  return new Error(BLOCKED_SYM, String::create(whatString));
}

Ref<Error> mica::permission_error(const char *whatString) {
  return new Error(PERMISSION_SYM, String::create(whatString));
}

Ref<Error> mica::divzero_error(const char *whatString) {
  return new Error(DIVZERO_SYM, String::create(whatString));
}

Ref<Error> mica::no_error(const char *whatString) {
  return new Error(NONE_SYM, String::create(whatString));
}

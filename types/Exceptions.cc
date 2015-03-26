/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/Exceptions.hh"

#include <cstdio>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "common/mica.h"
#include "types/Data.hh"
#include "types/Error.hh"
#include "types/GlobalSymbols.hh"
#include "types/String.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"

using namespace mica;

Ref<Error> mica::arguments_err(const char *whatString) {
  return new (aligned) Error(ARGUMENTS_SYM, String::create(whatString));
}

Ref<Error> mica::out_of_range(const char *whatString) {
  return new (aligned) Error(OUT_OF_RANGE_SYM, String::create(whatString));
}

Ref<Error> mica::unimplemented(const char *whatString) {
  return new (aligned) Error(UNIMPLEMENTED_SYM, String::create(whatString));
}

Ref<Error> mica::invalid_type(const char *whatString) {
  return new (aligned) Error(INVALID_TYPE_SYM, String::create(whatString));
}

Ref<Error> mica::slot_not_found(const char *whatString) {
  return new (aligned) Error(SLOT_NOT_FOUND_SYM, String::create(whatString));
}

Ref<Error> mica::slot_overload(const char *whatString) {
  return new (aligned) Error(SLOT_OVERLOAD_SYM, String::create(whatString));
}

Ref<Error> mica::not_found(const char *whatString) {
  return new (aligned) Error(NOT_FOUND_SYM, String::create(whatString));
}

Ref<Error> mica::stack_underflow(const char *whatString) {
  return new (aligned) Error(STACK_UNDERFLOW_SYM, String::create(whatString));
}

Ref<Error> mica::var_not_found(const char *whatString) {
  return new (aligned) Error(VAR_NOT_FOUND_SYM, String::create(whatString));
}

Ref<Error> mica::internal_error(const char *whatString) {
  return new (aligned) Error(INTERNAL_SYM, String::create(whatString));
}

Ref<Error> mica::max_ticks(const char *whatString) {
  return new (aligned) Error(MAX_TICKS_SYM, String::create(whatString));
}

Ref<Error> mica::terminated(const char *whatString) {
  return new (aligned) Error(TERMINATED_SYM, String::create(whatString));
}

Ref<Error> mica::max_recursion(const char *whatString) {
  return new (aligned) Error(MAX_RECURSION_SYM, String::create(whatString));
}

Ref<Error> mica::parse_error(const char *whatString) {
  return new (aligned) Error(PARSE_SYM, String::create(whatString));
}

Ref<Error> mica::block_error(const char *whatString) {
  return new (aligned) Error(BLOCKED_SYM, String::create(whatString));
}

Ref<Error> mica::permission_error(const char *whatString) {
  return new (aligned) Error(PERMISSION_SYM, String::create(whatString));
}

Ref<Error> mica::divzero_error(const char *whatString) {
  return new (aligned) Error(DIVZERO_SYM, String::create(whatString));
}

Ref<Error> mica::no_error(const char *whatString) {
  return new (aligned) Error(NONE_SYM, String::create(whatString));
}

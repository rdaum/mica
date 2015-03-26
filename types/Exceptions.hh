/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef EXCEPTIONS_HH
#define EXCEPTIONS_HH

#include <vector>

#include "types/Error.hh"

namespace mica {
class Var;

extern Ref<Error> arguments_err(const char *whatString);
extern Ref<Error> out_of_range(const char *whatString);
extern Ref<Error> unimplemented(const char *whatString);
extern Ref<Error> invalid_type(const char *whatString);
extern Ref<Error> slot_not_found(const char *whatString);
extern Ref<Error> slot_overload(const char *whatString);
extern Ref<Error> not_found(const char *whatString);
extern Ref<Error> stack_underflow(const char *whatString);
extern Ref<Error> var_not_found(const char *whatString);
extern Ref<Error> internal_error(const char *whatString);
extern Ref<Error> max_ticks(const char *whatString);
extern Ref<Error> terminated(const char *whatString);
extern Ref<Error> max_recursion(const char *whatString);
extern Ref<Error> parse_error(const char *whatString);
extern Ref<Error> block_error(const char *whatString);

extern Ref<Error> permission_error(const char *whatString);
extern Ref<Error> divzero_error(const char *whatString);
extern Ref<Error> no_error(const char *whatString);
}

#endif /* EXCEPTIONS_HH */

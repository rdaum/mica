/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include "Data.hh"
#include "Exceptions.hh"
#include "Var.hh"
#include "NoReturn.hh"

using namespace mica;

Var NoReturn::instance(new (aligned) NoReturn());

rope_string NoReturn::tostring() const {
  throw internal_error("tostring() called on NoReturn");
}

rope_string NoReturn::rep() const {
  throw internal_error("rep() called on NoReturn");
}

rope_string NoReturn::serialize() const {
  throw internal_error("serialize() called on NoReturn");
}


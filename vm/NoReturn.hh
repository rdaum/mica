/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef NORETURN_HH
#define NORETURN_HH

#include "generic_vm_entity.hh"

namespace mica {

  /** NoReturn is returned from Closure and Message's perform
   *  in order to indicate that the returned value will come through
   *  the stack, and not via the normal return value.  A Hack,
   *  should be replaced soon.
   */
  class NoReturn
    : public generic_vm_entity
  {
  public:
    Type::Identifier type_identifier() const { return Type::NORETURN; }

  public:
    static Var instance;

    rope_string tostring() const;

    rope_string rep() const;

  public:
    rope_string serialize() const;
  };



}

#endif /* NORETURN_HH */


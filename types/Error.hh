/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MICA_ERROR_HH
#define MICA_ERROR_HH

#include "Scalar.hh"
#include "Var.hh"
#include "Symbol.hh"
#include "String.hh"

namespace mica
{
  /** Error: a symbol and a description.  Used to pass exceptions
   *  around as values.
   */
  class Error
    : public Scalar
  {
  public:
    Type::Identifier type_identifier() const { return Type::ERROR; }

  public:
    Symbol err_sym;
    Ref<String> desc;

  public:
    Error( const Symbol &err_sym, const Ref<String> &description );

    Error( const Error &from );

  public:
    bool operator==( const Error &rhs ) const;

    bool operator==( const Var &rhs ) const;

    bool operator<(const Var &v2) const;
  
    bool truth() const;

    Var add( const Var &rhs ) const;

    Var div( const Var &rhs ) const;

    Var mul( const Var &rhs ) const;

    Var sub( const Var &rhs ) const;

    Var mod( const Var &rhs ) const;

    Var neg() const;

    Var inc() const;

    Var dec() const;

    unsigned int length() const;

    int toint() const;
    float tofloat() const;

    mica_string tostring() const;

    mica_string rep() const;

    mica_string serialize() const;

    bool isNumeric() const;

  public:
    child_set child_pointers();
  };

}

#endif /* MICA_ERROR_HH */

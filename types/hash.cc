/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"
#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_map>
#else
#include <hash_map>
#endif

#include "Data.hh"
#include "Var.hh"
#include "Ref.hh"
#include "Symbol.hh"
#include "hash.hh"


using namespace mica;

unsigned int hash_var::operator ()( const Var &var ) const
{
  return var.hash(); 
};

unsigned int hash_symbol::operator()( const Symbol &sym ) const {
  return sym.hash();
}


unsigned int str_hash::operator ()( const mica_string &str ) const
{
  STD_EXT_NS::hash<const char *> hasher;
  return hasher(str.c_str());
};



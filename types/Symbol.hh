/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef SYMBOL_HH
#define SYMBOL_HH

#include "Atoms.hh"

namespace mica {

  /** Symbol represents an atomic, interned label.
   *   
   *  Symbol is stored in Var, and is not a heap allocated child of
   *  Data
   */
  class Symbol {
  public:    
    static Symbol create( const char *str );
    static Symbol create( const mica_string &str );

  public:
    bool         is_integer : 1;
    bool         is_float   : 1;
    bool         is_pointer : 1;
    Atoms::types type       : 3;
    unsigned int idx        : 26;

   
   
    Symbol( const _Atom &atom_conversion )
    {
      memcpy( this, &atom_conversion, sizeof( atom_conversion ) );
      assert( type == Atoms::SYMBOL);
    }

  public:
    Symbol() 
      : is_integer(false),  is_float(false),is_pointer(false),
	type(Atoms::SYMBOL),
	idx(0) {}

    Symbol( const Symbol &symbol ) :
      is_integer(false), is_float(false), is_pointer(false), 
      type(Atoms::SYMBOL), idx(symbol.idx) {}

    Symbol &operator=( const Symbol &symbol ) {
      if (&symbol != this) {
	memcpy( this, &symbol, sizeof(symbol) );
      }
      return *this;
    }

    bool operator<( const Symbol &symbol ) const {
      return idx < symbol.idx;
    }

    bool operator==( const Symbol &symbol ) const {
      return idx == symbol.idx;
    }
    
    unsigned int hash() const {
      return STD_EXT_NS::hash<int>()(idx);
    }

    mica_string tostring() const;

    mica_string serialize() const;

  };
}

#endif

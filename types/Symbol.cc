#include "../config.h"
#include "common/mica.h"
 
#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_map>
#else
#include <hash_map>
#endif

#include <vector>


#include "Var.hh"
#include "Symbol.hh"
#include "hash.hh"

using namespace std;
using namespace mica;

typedef STD_EXT_NS::hash_map<mica_string, unsigned int,
			     str_hash > SymbolMap;
static SymbolMap symbol_map;
std::vector<mica_string> symbols;

Symbol Symbol::create( const char *c_str ) {
  Symbol sym;

  mica_string str(c_str);

  SymbolMap::iterator sym_i = symbol_map.find( str );
  if (sym_i == symbol_map.end()) {

    unsigned int idx = symbols.size();
    sym.idx = idx;
    symbol_map.insert( make_pair( str, idx ) );

    symbols.push_back( str );
  } else {
    sym.idx = sym_i->second;
  }

  return sym;
}

mica_string Symbol::tostring() const {
  return symbols[idx];
}

mica_string Symbol::serialize() const {
  mica_string s_form;
  Pack( s_form, Type::SYMBOL );
  s_form.append( symbols[idx] );

  return s_form;
}

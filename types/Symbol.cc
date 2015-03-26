#include "types/Symbol.hh"

#include <unordered_map>
#include <vector>

#include "common/mica.h"
#include "types/hash.hh"
#include "types/Var.hh"

using namespace std;
using namespace mica;

typedef std::unordered_map<mica_string, unsigned int, str_hash> SymbolMap;
static SymbolMap symbol_map;
std::vector<mica_string> symbols;

Symbol Symbol::create(const char *c_str) { return Symbol::create(mica_string(c_str)); }

Symbol Symbol::create(const mica_string &str) {
  Symbol sym;

  SymbolMap::iterator sym_i = symbol_map.find(str);
  if (sym_i == symbol_map.end()) {
    unsigned int idx = symbols.size();
    sym.idx = idx;
    symbol_map.insert(make_pair(str, idx));

    symbols.push_back(str);
  } else {
    sym.idx = sym_i->second;
  }

  return sym;
}

mica_string Symbol::tostring() const { return symbols[idx]; }

serialize_buffer Symbol::serialize() const {
  serialize_buffer s_form;

  Pack(s_form, Type::SYMBOL);
  s_form.append(symbols[idx]);

  return s_form;
}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/MetaObjects.hh"

#include <unordered_map>

#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/Map.hh"
#include "types/Object.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"

using namespace mica;
using namespace std;

typedef std::unordered_map<unsigned int, var_vector> MetadelegatesMap;
static MetadelegatesMap meta_delegates;

Var MetaObjects::TypeMeta;
Var MetaObjects::AtomMeta;
Var MetaObjects::SymbolMeta;
Var MetaObjects::SequenceMeta;
Var MetaObjects::StringMeta;
Var MetaObjects::MapMeta;
Var MetaObjects::SetMeta;
Var MetaObjects::ListMeta;
Var MetaObjects::ErrorMeta;
Var MetaObjects::SystemMeta;
Var MetaObjects::Lobby;
Var MetaObjects::AnyMeta;

#define INIT_META(NAME)                                            \
  NAME##Meta = Object::create();                                   \
  Lobby.declare(Var(NAME_SYM), Symbol::create(#NAME), NAME##Meta); \
  NAME##Meta.declare(NAME##Meta, TITLE_SYM, Var(Symbol::create(#NAME)));

#define INIT_META_CLONE(NAME, PARENT)                              \
  NAME##Meta = PARENT##Meta.clone();                               \
  Lobby.declare(Var(NAME_SYM), Symbol::create(#NAME), NAME##Meta); \
  NAME##Meta.declare(NAME##Meta, TITLE_SYM, Var(Symbol::create(#NAME)));

void MetaObjects::cleanup() {
  TypeMeta = NONE;
  AtomMeta = NONE;
  SymbolMeta = NONE;
  SequenceMeta = NONE;
  StringMeta = NONE;
  MapMeta = NONE;
  SetMeta = NONE;
  ListMeta = NONE;
  ErrorMeta = NONE;
  SystemMeta = NONE;

  meta_delegates.clear();
}

void MetaObjects::initialize(const Var &lobby) {
  Lobby = lobby;

  INIT_META(Type);

  INIT_META(System);

  INIT_META(Any);

  INIT_META_CLONE(Atom, Type);

  INIT_META_CLONE(Symbol, Atom);

  INIT_META_CLONE(Error, Atom);

  INIT_META_CLONE(Sequence, Type);

  INIT_META_CLONE(List, Sequence);

  INIT_META_CLONE(Map, Sequence);

  INIT_META_CLONE(Set, Sequence);

  INIT_META_CLONE(String, Sequence);

  meta_delegates[Type::INTEGER].push_back(AtomMeta);
  meta_delegates[Type::FLOAT].push_back(AtomMeta);
  meta_delegates[Type::CHAR].push_back(AtomMeta);
  meta_delegates[Type::OPCODE].push_back(AtomMeta);
  meta_delegates[Type::BOOL].push_back(AtomMeta);

  meta_delegates[Type::SYMBOL].push_back(SymbolMeta);
  meta_delegates[Type::ERROR].push_back(ErrorMeta);
  meta_delegates[Type::LIST].push_back(ListMeta);
  meta_delegates[Type::MAP].push_back(MapMeta);
  meta_delegates[Type::SET].push_back(SetMeta);
  meta_delegates[Type::STRING].push_back(StringMeta);
}

child_set mica::global_roots() {
  child_set roots;

  roots << MetaObjects::TypeMeta << MetaObjects::AtomMeta << MetaObjects::SequenceMeta
        << MetaObjects::ListMeta << MetaObjects::StringMeta << MetaObjects::MapMeta
        << MetaObjects::SetMeta << MetaObjects::SymbolMeta << MetaObjects::ErrorMeta
        << MetaObjects::SystemMeta << MetaObjects::Lobby << MetaObjects::AnyMeta;

  return roots;
}

var_vector MetaObjects::delegates_for(Type::Identifier type_id) {
  MetadelegatesMap::iterator fi = meta_delegates.find((unsigned int)type_id);

  if (fi == meta_delegates.end())
    return var_vector();
  else
    return fi->second;
}

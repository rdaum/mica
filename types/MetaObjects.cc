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
#include "Object.hh"

#include "Exceptions.hh"
#include "Symbol.hh"
#include "GlobalSymbols.hh"

#include "Pool.hh"
#include "Pools.hh"

#include "MetaObjects.hh"

using namespace mica;
using namespace std;

typedef STD_EXT_NS::hash_map<unsigned int, var_vector> MetadelegatesMap;
static MetadelegatesMap meta_delegates;

Var MetaObjects::TypeMeta;
Var MetaObjects::ScalarMeta;
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

#define INIT_META(NAME) \
  NAME ##Meta = Object::create(); \
  Lobby.declare( Var(NAME_SYM), Symbol::create( #NAME ), NAME ##Meta ); \
  NAME ##Meta.declare( NAME ##Meta, TITLE_SYM, Var(Symbol::create( # NAME )) );

#define INIT_META_CLONE(NAME, PARENT) \
  NAME ##Meta = PARENT ##Meta.clone(); \
  Lobby.declare( Var(NAME_SYM), Symbol::create( #NAME ), NAME ##Meta ); \
  NAME ##Meta.declare( NAME ##Meta, TITLE_SYM, Var(Symbol::create( # NAME )) );

void MetaObjects::cleanup() {
  TypeMeta = NONE;
  ScalarMeta = NONE;
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

void MetaObjects::initialize( const Var &lobby ) {
  Lobby = lobby;
  
  INIT_META(Type);

  INIT_META(System);

  INIT_META(Any);

  INIT_META_CLONE(Scalar, Type);

  INIT_META_CLONE(Symbol, Scalar);

  INIT_META_CLONE(Error, Scalar);

  INIT_META_CLONE(Sequence, Type);

  INIT_META_CLONE(List, Sequence);

  INIT_META_CLONE(Map, Sequence);

  INIT_META_CLONE(Set, Sequence);

  INIT_META_CLONE(String, Sequence);

  meta_delegates[Type::INTEGER].push_back( ScalarMeta );
  meta_delegates[Type::FLOAT].push_back( ScalarMeta );
  meta_delegates[Type::CHAR].push_back( ScalarMeta );
  meta_delegates[Type::OPCODE].push_back( ScalarMeta );
  meta_delegates[Type::BOOL].push_back( ScalarMeta );

  meta_delegates[Type::SYMBOL].push_back( SymbolMeta );
  meta_delegates[Type::ERROR].push_back( ErrorMeta );
  meta_delegates[Type::LIST].push_back( ListMeta );
  meta_delegates[Type::MAP].push_back( MapMeta );
  meta_delegates[Type::SET].push_back( SetMeta  );
  meta_delegates[Type::STRING].push_back( StringMeta );
}

child_set mica::global_roots() {
  child_set roots;

  roots << 
    MetaObjects::TypeMeta << 
    MetaObjects::ScalarMeta << 
    MetaObjects::SequenceMeta << 
    MetaObjects::ListMeta << 
    MetaObjects::StringMeta << 
    MetaObjects::MapMeta << 
    MetaObjects::SetMeta << 
    MetaObjects::SymbolMeta << 
    MetaObjects::ErrorMeta << 
    MetaObjects::SystemMeta << 
    MetaObjects::Lobby << 
    MetaObjects::AnyMeta;

  return roots;
}
  
var_vector MetaObjects::delegates_for( Type::Identifier type_id ) {
  MetadelegatesMap::iterator fi = meta_delegates.find( (unsigned int)type_id );
  
  if (fi == meta_delegates.end())
    return var_vector();
  else
    return fi->second;
}

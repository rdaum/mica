/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"
#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
#  include <ext/hash_map>
#else
#  include <hash_map>
#endif

#include <cassert>
#include <cstdio>


#include "Data.hh"
#include "Var.hh"
#include "Symbol.hh"
#include "Pools.hh"
#include "Scalar.hh"
#include "Error.hh"
#include "Exceptions.hh"

#include "Object.hh"
#include "Pool.hh"

#include "Environment.hh"
 




using namespace mica;
using namespace std;

/** Static global singleton
 */
Pools Pools::instance;

Pools::Pools() {
  _pools.clear();
  default_pool = 0;
}

Pools::~Pools()
{
  NamesMap::iterator ni;
  for (ni = names.begin(); ni != names.end(); ni++) {
    Pool* x = _pools[ni->second];
    delete x;
  }

  names.clear();
  _pools.clear();
}

std::vector<Pool*> Pools::pools() const
{
  return _pools;
}

Pool *Pools::get( PID pool ) const
{
  Pool *poolO;
  if (pool >= _pools.size() || !(poolO = _pools[pool])) {
    char errstr[50];
    snprintf( errstr, 50, "pool %d not found", pool);
    throw internal_error(errstr);
  }
  return poolO;
}

void Pools::removePool( PID pool )
{
  if (pool >= _pools.size())
    throw internal_error("pool not found");

  _pools[pool] = (Pool*)0;
  NamesMap::iterator ni;
  for (ni = names.begin(); ni != names.end(); ni++) 
    if (ni->second == pool)
      break;
      
  if (ni != names.end())
    names.erase(ni);
  else
    throw internal_error("name mapping for pool not found");
}

PID Pools::add( const Symbol &name, Pool *pool )
{
  PID idx = _pools.size();
  _pools.push_back( pool );
  names[name] = idx;

  return idx;
}


void Pools::setDefault( PID pool )
{
  default_pool = pool;
}

PID Pools::getDefault() const
{
  return default_pool;
}


Pool *Pools::find_pool_by_name( const Symbol &poolName ) const
{
  NamesMap::const_iterator ni;
  ni = names.find( poolName );

  if (ni == names.end())
    throw not_found("object pool not found");
  
  return get(ni->second);
}


void Pools::remove( const Var &obj )
{
  if (obj.type_identifier() != Type::OBJECT)
    throw invalid_type("unable to remove non-object from pool");


  Ref<Object> handle = obj->asRef<Object>();

  return get(handle->pid)->eject( handle->oid );
}


void Pools::close()
{
  /** Don't close the first one
   */
  vector<Pool*>::iterator pi;
  for (pi = _pools.begin() + 1; pi != _pools.end(); pi++) {
    (*pi)->close();
    delete (*pi);
  }    
}

void Pools::sync()
{
  vector<Pool*>::iterator pi;
  for (pi = _pools.begin(); pi != _pools.end(); pi++) {
    (*pi)->sync();
  }    
}

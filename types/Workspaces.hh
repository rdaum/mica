/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef POOLS_HH
#define POOLS_HH

#include <vector>

#include "config.h"
#include "common/mica.h"

#ifdef HAVE_EXT_HASH_MAP
#  include <ext/hash_map>
#else
#  include <hash_map>
#endif

#include "hash.hh"

namespace mica 
{
  typedef unsigned int PID;
  typedef unsigned int OID;

  class Pool;

  class Pools
  {
  public:  
    /** The global static singleton.
     */
    static Pools instance;

    Pools();

    ~Pools();

  public:
    /** Return a list of active pools.
     */
    std::vector<Pool*> pools() const ;

    Pool* get( PID pool ) const ;

    void removePool( PID pool );

    PID add( const Symbol &name, Pool *pool );

    /** Close all pools.
     */
    void close();

    /** Sync all pools
     */
    void sync();

  public:
    /** Set the default pool
     */
    void setDefault( PID pool );

    /** Return the current default pool
     */
    PID getDefault() const;

  public:
    void remove( const Var &obj );

    Pool *find_pool_by_name( const Symbol &poolName ) const;

  private:
    std::vector<Pool*> _pools;

    typedef STD_EXT_NS::hash_map<Symbol, PID, hash_symbol> NamesMap;
    
    NamesMap names;

    PID default_pool;
  };

};

#endif

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef TYPES_WORKSPACES_HH
#define TYPES_WORKSPACES_HH

#include <boost/pool/pool_alloc.hpp>
#include <unordered_map>
#include <vector>

#include "types/hash.hh"

namespace mica {
typedef unsigned int WID;
typedef unsigned int OID;

class Workspace;

class Workspaces {
 public:
  /** The global static singleton.
   */
  static Workspaces instance;

  Workspaces();

  ~Workspaces();

 public:
  /** Return a list of active pools.
   */
  std::vector<Workspace *> pools() const;

  Workspace *get(WID pool) const;

  void removePool(WID pool);

  WID add(const Symbol &name, Workspace *pool);

  /** Close all pools.
   */
  void close();

  /** Sync all pools
   */
  void sync();

 public:
  /** Set the default pool
   */
  void setDefault(WID pool);

  /** Return the current default pool
   */
  WID getDefault() const;

 public:
  void remove(const Var &obj);

  Workspace *find_pool_by_name(const Symbol &poolName) const;

 private:
  std::vector<Workspace *> workspaces_;

  typedef std::unordered_map<Symbol, WID, hash_symbol, std::equal_to<Symbol>> NamesMap;
  NamesMap names_;

  WID default_workspace_;
};

};  // namespace mica

#endif  // TYPES_WORKSPACES_HH

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef MICA_PERSISTENT_WORKSPACE_HH
#define MICA_PERSISTENT_WORKSPACE_HH

#include <boost/tuple/tuple.hpp>
#include <lmdb.h>

#include "types/Workspace.hh"

namespace mica {

class Workspace;

/** An object pool which provides transparent persistence services via
 *  an object database.  See the class documentation for Workspace for more
 *  description of the method functionality.
 *  @see Workspace
 */
class PersistentPool : public Workspace {
 public:
  /** Open a new persistent pool.
   */
  static boost::tuple<WID, Var> open(const Symbol &name,
                                     const Ref<Object> &parent_lobby = Ref<Object>(0));
  void sync();

  void close();

  Object *new_object();

  OStorage *get_environment(OID object_id);

 protected:
  Ref<Object> resolve(OID index);

  void del(OID idx);

  void write(OID oid);

  bool exists(OID id);

 protected:
  struct CacheEntry {
    OID object_id;
    unsigned int usecnt;
    bool deleted;

    CacheEntry() : object_id(0), usecnt(0), deleted(true) {}

    CacheEntry(OID oid, int uses) : object_id(oid), usecnt(uses), deleted(false) {}

    /** Backwards from what you think, so that higher usecounts
     *  appear first in sorting.
     */
    bool operator<(const CacheEntry &rhs) const { return usecnt > rhs.usecnt; }

    bool operator==(const CacheEntry &rhs) const {
      return object_id == rhs.object_id && usecnt == rhs.usecnt;
    }
  };
  typedef std::vector<CacheEntry> CacheVector;
  CacheVector cache_list_;

  void flush_cache();
  void push_cache(OID oid);

  size_t cache_width;
  size_t cache_grow_window;

 public:
  /** Constructor is protected -- must open a pool through the
   *  static ::open function.
   */
  PersistentPool(const Symbol &poolName);

  void initialize();

  void write_object(OID oid);

 public:
  void load_tasks();

  void save_tasks();

 private:
  enum { ENV_DB, OID_DB } dbs;
#define NUM_DBS (OID_DB + 1)
  MDB_env *db_env_[NUM_DBS];
  MDB_txn *db_txn_[NUM_DBS];
  MDB_dbi db_dbi_[NUM_DBS];
  mica_string names_[NUM_DBS];
};

}  // namespace mica

#endif   // MICA_PERSISTENT_WORKSPACE_HH


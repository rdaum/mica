/** Copyright 2002, Ryan Daum.
 */
#ifndef ANTICYCLIC_REFCOUNTED_HH
#define ANTICYCLIC_REFCOUNTED_HH

#include "common/mica.h"
#include "config.h"

#include <list>

namespace mica {


  class reference_counted;
  typedef std::list<reference_counted*> child_set;

  /** Base class for reference counted garbage collection.
   *  Use in conjunction with a smart pointer (@see Ref<> and @see Var)
   *  to provide automatic memory reclamation for unused objects.
   *  
   *  Descendants must provide a method 'child_pointers' which must return
   *  a list of all other reference counted objects contained within
   *  this object.  This list is used by the cycle detector (see below)
   *  to reclaim cyclic dependencies.
   *
   *  Plain-vanilla reference counting cannot handle cylic dependencies
   *  without using weak-references, which are not a complete solution.
   *  In generic reference count memory management, objects must maintain
   *  a tree (rather than a graph) layout.
   *
   *  Cycle detection allows for full graph linkages.  This particular class
   *  incorporates a cycle detection algorithm described by David F. Bacon
   *  and V.T.Rajan at:
   *
   *  http://www.research.ibm.com/people/d/dfb/papers/Bacon01Concurrent.pdf
   * 
   *  "In this section we describe our synchronous cycle collection
   *   algorithm, which applies the same principles as those of Martinez et.
   *   al and Lins, but which only requires O(N + E) worst-case time for
   *   collection (where N is the number of nodes and E is the number of
   *   edges in the object graph), and is therefore competitive with tracing
   *   garbage collectors."
   */
  class reference_counted
  {
  public:
    typedef enum { BLACK,    // In use or free
		   GRAY,     // Possible member of cycle
		   WHITE,    // Member of garbage cycle
		   PURPLE,   // Possible root of cycle
		   GREEN     // Acyclic

		   /** Unimplemented (for concurrent version of
		       algorithm ) 
		       RED,      // Candidate cycle undergoing computation
		       ORANGE    // Candidate cycle awaiting epoch boundary
		   */

    } Colouring;
    
    /** storage of the reference count for a piece of Data
     */
    int refcnt        : 27;
    bool buffered     : 1;
    bool paged        : 1;
    Colouring colour  : 3;
    
    
  public:
    reference_counted();
    virtual ~reference_counted();

  public:
    // EXTERNAL REFERENCE COUNT METHODS

    /** Decrease the reference count.  Called by reference counting smart
     *  pointers when the reference goes out of scope.
     *  TODO: immediately reclaim acyclic (green) objects.
     */
    void dncount();

    /** Increase the reference count.  CAlled by reference counting smart
     *  pointers when a reference to an object is acquired by assignment
     *  or copy.
     *  TODO: don't mark acyclic (green) objects black.
     */
    void upcount();

    /** Initiate a scan to search for cycles in the refcount tree.
     *  Class-global static method.
     */
    static void collect_cycles();

  private:
    // CYCLE RECLAMATION METHODS

    /** See detailed descriptions of this algorithm at:
     *  http://www.research.ibm.com/people/d/dfb/papers/Bacon01Concurrent.pdf
     *  (pages 5 - 7)
     */

    /** Release a non-buffered nodes.  Buffered nodes are possible roots.
     */
    void release();

    /** Called by dncount() if an object's reference count doesn't hit zero
     *  at dncount.  Based on the philosophy that ``objects die young'',
     *  anything which doesn't die right away is perhaps a root of a cycle.
     */
    void possible_root(); 

    /** Invoked by collect_cycles to attempt to mark roots and unbuffer
     *  obviously non-root objects.  
     */
    static void mark_roots();

   /** Visits the graph from the roots and marks things gray and reduces
     *  their reference counts.  Should "shake up the tree" and find
     *  all the rotten apples, so to speak.
     */
    void mark_gray();

    /** Scans all those grayed nodes looking for live or non-live
     *  objects
     */
    static void scan_roots();

    /** Distinguishes between live and non-live rooted objects.  Calls
     *  scan_black to recolour the hierarchy of live objects.  Everything
     *  else is coloured white -- for possible destruction.
     */
    void scan(); 
    
    /** Scan black nodes and recolouring them back to black and restoring
     *  their reference counts
     */
    void scan_black();

    /** Now that we've marked the white (cyclic) nodes, we need to go through
     *  and unbuffer and remove them from the roots buffer.
     */
    static void collect_roots();

    /** Free all the garbage nodes.
     */
    void collect_white();

  public:
    /** Return a list of child pointers from this object
     */
    virtual child_set child_pointers() = 0;

  public:
    /** Called right before an object is physically freed
     */
    virtual void finalize_object() {
    }

    /** Called right before a page object is freed, and right after
     *  finalize_object is called.
     */
    virtual void finalize_paged_object() {
    }

  public:
    bool paging() const;

  };

  /** When paging objects in and out of cache, all reference counting to
   *  page-managed objects should be nullified.  These functions are here
   *  for the cache to toggle the paging flag.  This is definitely not
   *  thread safe or re-entrant.
   */
  extern void notify_start_paging();
  extern void notify_end_paging();

  /** Returns the status of the cycle collector
   *  @param return true if the cycle collector is currently enabled
   */
  extern bool cycle_collecting();

}

#endif /** ANTICYCLIC _REFCOUNTED_HH **/

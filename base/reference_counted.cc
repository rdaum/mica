/** Copyright 2002, Ryan Daum 
 */

/** Implementation of reference counted base class.  See header for 
 *  documentation
 */
#include <typeinfo>
#include <algorithm>
#include <functional>
#include <iostream>
#include <set>

#include "reference_counted.hh"

#include "logging.hh"

/** Bring these two namespaces into scope, for convenience
 */
using namespace mica;
using namespace std;

/** Global COLLECTING flag -- set during cycle collection to notify the
 *  cache that it shouldn't do any page outs during cycle collections.
 *  This is _absolutely_ not thread safe.
 */
static bool COLLECTING = false;
bool mica::cycle_collecting() {
  return COLLECTING;
}

/** Global FREEING flag -- set during frees to prevent further auto refcounts
 *  from taking hold.
 *  Again not thread safe.
 */
static bool FREEING = false;

/** Global paging flag.  See notes about thread safety above.
 */
static bool PAGING = false;

void mica::notify_start_paging() {
  PAGING = true;
}
    
void mica::notify_end_paging() {
  PAGING = false;
}


inline bool reference_counted::paging() const {
  return PAGING && this->paged;
}

/** This is the list of roots.  It's a linked list, for quick inplace
 *  insertions / removals.
 */
static child_set Roots;

/** Objects that are garbage, to delete
 */
static vector<reference_counted*> GarbageList;

/** Add the object in question to the garbage list, to be freed later.
 */
static void Free( reference_counted *who )
{
  if (!who->garbaged) {
    who->garbaged = true;
    GarbageList.push_back( who );
  }
}

/** Take out the trash.
 */
static void free_garbage() {

  /** Engage lock
   */
  FREEING = true;

  for (vector<reference_counted*>::iterator x = GarbageList.begin();
       x != GarbageList.end(); x++) {

    reference_counted *who = *x;

    /** Do not free paged objects while PAGING is in effect
     */
    if (!who->paging()) {

      who->finalize_object();
    
      if (who->paged)
	who->finalize_paged_object();
      
      delete who;
    }   
  }
  /** Remove lock
   */
  FREEING = false;

  GarbageList.clear();
}

/** We initialize everything with an initial reference count of 0 and
 *  a BLACK colour
 */
reference_counted::reference_counted()
  : refcnt(0),
    buffered(false),
    paged(false),
    garbaged(false),
    colour(BLACK)
{}

reference_counted::~reference_counted() {}


/** SEE DOCUMENTATION IN PAPER FOR ALGORITHM AND FUNCTION DESCRIPTIONS
 */
void reference_counted::mark_roots() {

  for (child_set::iterator x = Roots.begin();
       x != Roots.end();) {
   
    reference_counted *S = *x;
 
    child_set::iterator next = x;
    next++;
   
    if (S) { // Only visit non-NULL nodes, just in case garbage
      // gets in here

      if (S->colour == reference_counted::PURPLE) {
       
	S->mark_gray();
       
      } else {
       
	Roots.erase( x );

	S->buffered = false;
	if (S->colour == reference_counted::BLACK && (S->refcnt == 0)) {
	  Free(S);
	}
       
      }
    }
   
    x = next;

  }

}

void reference_counted::scan_roots() {

  for (child_set::iterator mr = Roots.begin(); 
       mr != Roots.end(); mr++) {
    reference_counted *S = *mr;
    if (S)
      S->scan();
  }  

}

void reference_counted::collect_roots() {

  for (child_set::iterator mr = Roots.begin(); 
       mr != Roots.end(); mr++) {

    reference_counted *S = *mr;

    if (S->paging())
      continue;

    S->buffered = false;     // Unbuffer it.
    S->collect_white();
  }  

  Roots = global_roots();

}

void reference_counted::collect_cycles() {

  logger.debugStream() << "collecting cycles" << 
    log4cpp::CategoryStream::ENDLINE;

  /** Set collecting flag.  This is here so that the cache algorithm will not
   *  attempt to page out objects while a collection cycle is in process.
   */
  COLLECTING = true;

  /** Lock this for protection.  Can't have people calling us while
   *  we're already invoked.
   */
  static bool locked = false;

  if (!locked)
    locked = true;
  else {
    return;
  }


  // Remove internal reference counts
  mark_roots();

  // Restore reference counts that are non-zero
  scan_roots();

  // Collect cyclic garbage
  collect_roots(); 

  // Do actual free
  free_garbage();

  /** Unlock the cycle collector now.
   */
  locked = COLLECTING = false;
}


void reference_counted::upcount() { 
  if (FREEING || paging())
    return;

  /** Increment the refcount.  Would be nice if this was made more
   *  atomic.
   */
  refcnt++;

  //  refcnt++;

  /**  Keep GREEN (guaranteed non-cyclic) nodes from blackening!
   */
  if (colour != GREEN)
    colour = BLACK;
} 
 
void reference_counted::dncount() {

  if (FREEING || paging())
    return;

  refcnt--; // reduce the reference count

  /** Green nodes should be freed without checking for possible roots
   *  They can't be part of a cycle.
   */
  if (colour == GREEN && refcnt == 0)
    Free(this);  // Bye bye greeny.
  else {
    /** The idea here is that if this guy is still around (non zero refcnt)
     *  then there's a good chance (statistically) that it could be a 
     *  cycle member.  We trace it to find out.
     */
    if (refcnt == 0) 
      release(); 
    else 
      possible_root();  
  }
} 

void reference_counted::release() {

  /** Decrement children
   */
  child_set children(child_pointers());
  for (child_set::iterator x = children.begin();
       x != children.end(); x++) {
    reference_counted *S = *x;
    if (S)
      S->dncount();
  }
  
  /** We only release non-buffered nodes
   */
  colour = BLACK;
  if (!buffered)
    Free(this); // Bye bye

}



void reference_counted::possible_root() {

  // If colour is already purple then I'm a candidate root.  If not,
  // colour me purple and scan my friends.  Don't look at greens, they
  // are non-cyclic by nature.
  if (colour != PURPLE && colour != GREEN) {
    colour = PURPLE;

    if (!buffered) {
      buffered = true;

      /** This is a root.  Maybe.
       */
      Roots.push_back( this );
    }
  }
}

void reference_counted::scan() {

  if (paging())
    return;

  if (colour == GRAY) {
    
    if (refcnt > 0) {
      scan_black();

    } else {
      colour = WHITE;

      /** RECURSION.  Would be nice if we could roll this out into (TODO) 
       *  something iterative.
       */
      child_set children(child_pointers());
      for (child_set::iterator x = children.begin();
	   x != children.end(); x++) {
	(*x)->scan();
      }
      
      /** I rolled the for_each out to be explicit
       *  so that debugging would be easier
       *  (STL likes to confuse GDB.)
       */

      //      for_each( children.begin(), children.end(), 
      //		mem_fun( &reference_counted::scan ) );

    }
  }
}

/** Another recursive function.  See TODO above.
 */
void reference_counted::scan_black() {
  if (paging())
    return;

  colour = BLACK;

  child_set children(child_pointers());
  for (child_set::iterator x = children.begin();
       x != children.end(); x++) {
    reference_counted *S = *x;

    if (S) {
      S->refcnt++;        // increase refcnt
      
      if (S->colour != BLACK)
	S->scan_black();          // At it again.
    }
  }
}

/** Yet another recursive function.  See TODO above.
 */
void reference_counted::mark_gray() {
  if (paging())
    return;

  if (colour != GRAY && colour != GREEN) {
    colour = GRAY;

    child_set children(child_pointers());
    for (child_set::iterator x = children.begin();
	 x != children.end(); x++) {
      reference_counted *S = *x;
      if (S) {
	S->refcnt--;    // decrement refcnt
	
	S->mark_gray();
      }
    }
  }
}

void reference_counted::collect_white() {
  if (paging())
    return;

  if (colour == WHITE && !buffered) {
    colour = BLACK;
    child_set children(child_pointers());

    /** Recursion.
     */
    for_each( children.begin(), children.end(),
	      mem_fun( &reference_counted::collect_white ) );

    Free(this);       // Bye bye, cycle!
  }
}

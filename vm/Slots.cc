/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"
#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_set>
#else
#include <hash_set>
#endif

/** For MAXINT
 */
#include <boost/cstdint.hpp>

#include "Data.hh"
#include "Var.hh"
#include "Object.hh"
#include "Exceptions.hh"
#include "GlobalSymbols.hh"
#include "AbstractBlock.hh"
#include "List.hh"
#include "MetaObjects.hh"
#include "Slots.hh"

using namespace mica;
using namespace std;


/** This is the current dispatch generation.  It's a global static
 *  and this means that the whole dispatch process isn't at all re-entrant.
 *  Oh well, SUFFER.
 *
 *  It is used to differentiate different runs of the dispatch/lookup
 *  process.  We do this so we can store the bitsets and delegation
 *  stacks directly on the methods and objects respectively, and just
 *  clear them lazily when the next dispatch happens.  This way we
 *  can avoid expensive hash/map lookups for every dispatch
 */
static int generation = 0;
inline void increment_generation() {
  if (generation == UINT_MAX) generation = 0;
  else generation++;
}


/** For keeping track of visits so we don't visit the same object twice
 *  We hash only on the "unique id" which is really a literal memcpy
 *  from memory of the Var -- objects with the same type and pointer
 *  will compare as the same; thus, fast pointer identity, not value-identity,
 *  which is really what we want in this case.
 */
typedef STD_EXT_NS::hash_set<Var, hash_var> VisitedMap;

Slot Slots::get_slot( const Var &self,
			    const Var &accessor, 
			    const Symbol &name ) 
{
  VisitedMap visited;

  /** Attempt to actually get the value locally.  
   */
  OptSlot res(self.get( accessor, name ));
  if (res)
    return *res;
     
  /** If we have no delegates, we can fail immediately.
   */
  var_vector delegates_vector(self.delegates());
  if (delegates_vector.empty())
    throw E_SLOTNF;

  do {  
    Var parent = delegates_vector.back();
    delegates_vector.pop_back();

    /** Never visit the same object twice
     */
    if (visited.find(parent) == visited.end()) {
      visited.insert( parent );

      res = parent.get( accessor, name );
      if (res)
	return *res;

      var_vector copy_vec(parent.delegates());
      size_t old = delegates_vector.size();
      delegates_vector.insert( delegates_vector.end(), copy_vec.rbegin(),
			       copy_vec.rend() );
      assert( delegates_vector.size() - old == copy_vec.size());
    } 
  } while (!delegates_vector.empty());

  throw E_SLOTNF;
}


Slot Slots::get_name( const Var &self,
			    const Symbol &name ) {
  return get_slot( self, Var(NAME_SYM), name );
}

Slot Slots::match_verb( const Var &self,
			      const Symbol &name,
			      const var_vector &arguments ) {

  increment_generation();

  vector< Ref<Object> > delegations[64]; // the delegation dispatch stacks
 
  /** If the arguments are zilch, then just attempt immediate dispatch
   */
  if (arguments.empty())
    return get_slot( self, List::empty(), name );

  var_vector args;
  args.push_back( self );
  args.insert( args.end(), arguments.begin(), arguments.end() );

  unsigned int num_args = args.size();
  for (unsigned int pos = 0; pos < num_args; ++pos) {
    delegations[pos].clear();
  }
 
  bool delegated = false;
  ArgumentMask triedAny;


  do {
    delegated = false;
 
    for (var_vector::iterator A = args.begin(); A != args.end();) {
      unsigned int pos = A - args.begin();

      /** If it's not an object, skip it -- one of its delegates may
       *  handle this, but it can't!
       */
      if (A->type_identifier() == Type::OBJECT) {
	Ref<Object> rA = (*A)->asRef<Object>();

	/** Get all parasites for this verb in this position on
	 *  this argument
	 */
	VerbList candidates( rA->get_verb_parasites( name, pos ) );
	for (VerbList::iterator Co = candidates.begin();
	     Co != candidates.end(); Co++) {
	
	  /** Argument template for the verbdef must match the
	   *  length of our in-arguments
	   */
	  if ( (*Co)->argument_template.size() == (num_args - 1)) {

	    Ref<AbstractBlock> method( (*Co)->method->asRef<AbstractBlock>() );
	    
	    /** Mark this method as searched within this generation
	     */
	    if (method->arg_mask.dispatch_generation != generation) {
	      method->arg_mask.clear();
	      method->arg_mask.dispatch_generation = generation;
	    }
	    
	    /** Mark the argument position as visited
	     */
	    method->arg_mask.mark_argument( pos );
	    
	    /** It's all matched -- return the method
	     */
	    if (method->arg_mask.marked_all_of( num_args ) ) 
	      return Slot( (*Co)->definer, (*Co)->method );

	  }
	}
      }


      /** There was no dispatch, so try with some delegates
       */
      var_vector Delegates( A->delegates() );

      if (!Delegates.empty()) {

	for (var_vector::iterator Do = Delegates.begin();
	     Do != Delegates.end(); Do++) {

	  Ref<Object> D( (*Do)->asRef<Object>() );

	  if (Do == Delegates.begin()) {

	    // Retry dispatch on the Delegate
	    *A = *Do;
	    delegated = true;

	  } else {

	    delegations[pos].push_back( D );
	    D->arg_mask.clear();
	  }

	  /** If this object hasn't been visited in this dispatch
	   *  then we need to set the generation and clear its
	   *  arg mask
	   */
	  if (D->arg_mask.marked_argument( pos )) {
	    
	    // the object has already been visited as this
	    // argument position, skip it
	    continue;
	  }
	  D->arg_mask.mark_argument( pos );
	}

      } else {

	if (!delegations[pos].empty()) {

	  Var X = Var(delegations[pos].back());
	  delegations[pos].pop_back();

	  // Retry dispatch with X
	  *A = X;
	  delegated = true;

	} else {


	  // No more delegates to try, a dead-end.  Try Any.
	  // O
	  if (pos // Only try for non-self argument positions
	      && !triedAny.marked_argument(pos)) {
	    *A = MetaObjects::AnyMeta;
	    triedAny.mark_argument(pos);
	    delegated = true;
	  } else {
	    delegated = false;
	  }

	}
      }
    
      A++;
    }
  } while (delegated);

  throw E_SLOTNF;
}

Slot Slots::get_verb( const Var &self,
		      const Symbol &selector,
		      const var_vector &argument_template ) {
  OptSlot res(self.get( List::from_vector(argument_template), selector ));
  if (res)
    return *res;
  else
    throw E_SLOTNF;
}

vector< Ref<Object> > build_arguments( const Var &self,
				       const var_vector &argument_template ) {
  vector< Ref<Object> > arguments;
  arguments.push_back( self->asRef<Object>() );
  for (var_vector::const_iterator ai = argument_template.begin(); 
       ai != argument_template.end(); ai++) {
    if (ai->type_identifier() != Type::OBJECT)
      throw E_PERM;
    else
      arguments.push_back((*ai)->asRef<Object>());
  }
    
  return arguments;
}

void assign_arguments( const Var &self,
		       const vector< Ref<Object> > &arguments,
		       const Symbol &selector,
		       const var_vector &argument_template,
		       const Var &method ) {
  
  /** now go through the argument list and reset the verb parasites
   */
  for (vector< Ref<Object> >::const_iterator Or = arguments.begin();
       Or != arguments.end(); Or++) {
    unsigned int pos = Or - arguments.begin();
    (*Or)->set_verb_parasite( selector, pos, argument_template, 
			      self, method );
  }
}

Var Slots::declare_verb( Var &self,
			 const Symbol &selector,
			 const var_vector &argument_template,
			 const Var &method ) {

  /** copy args and verify that all items in the template are objects
   */
  if (self.type_identifier() != Type::OBJECT)
    throw E_PERM;

  vector< Ref<Object> >
    arguments( build_arguments( self, argument_template ) );

  /** declare its template locally
   */
  self.declare( List::from_vector(argument_template), selector, method );
  
  /** now go through the argument list and add the verb parasites
   */
  assign_arguments( self, arguments, selector, argument_template,
		    method );

  
  return method;
}


Var Slots::assign_verb( Var &self,
			const Symbol &selector,
			const var_vector &argument_template,
			const Var &method ) {
  
  /** If this fails, it wasn't declared.
   */
  self.assign( List::from_vector(argument_template), selector, method );

  /** copy args and verify that all items in the template are objects
   */
  if (self.type_identifier() != Type::OBJECT)
    throw E_PERM;

  vector< Ref<Object> >
    arguments( build_arguments( self, argument_template ) );
 
  /** now go through the argument list and reset the verb parasites
   */
  assign_arguments( self, arguments, selector, argument_template,
		    method );
  
  return method;
}

void Slots::remove_verb( Var &self,
			 const Symbol &selector,
			 const var_vector &argument_template ) {

  /** If this fails, it wasn't declared.
   */
  self.remove( List::from_vector(argument_template), selector );

  /** copy args and verify that all items in the template are objects
   */
  if (self.type_identifier() != Type::OBJECT)
    throw E_PERM;

  vector< Ref<Object> >
    arguments( build_arguments( self, argument_template ) );
 
  /** now go through the argument list and remove the verb parasites
   */
  for (vector< Ref<Object> >::iterator Or = arguments.begin();
       Or != arguments.end(); Or++) {
    unsigned int pos = Or - arguments.begin();
    (*Or)->rm_verb_parasite( selector, pos, argument_template );
  }
}

Slot Slots::get_delegate( const Var &self,
				const Symbol &name ) {
  return get_slot( self, Var(DELEGATE_SYM), name );
}

bool Slots::isA( const Var &self, const Var &prototype ) 
{
  VisitedMap visited;

  /** Compare with self first
   */
  if (self == prototype)
    return true;

  /** That failed.  Now start the iteration through delegates
   *  See notes from get_slot function above on implementation.
   */
  var_vector delegates_vector( self.delegates() );
  if (delegates_vector.empty())
    return false;

  do {  
    Var parent = delegates_vector.back();
    delegates_vector.pop_back();

    if (parent == prototype)
      return true;

    /** Never visit the same object twice
     */
    if (visited.find(parent) == visited.end()) {
      visited.insert(parent);
    
      var_vector copy_vec(parent.delegates());
      size_t old = delegates_vector.size();
      delegates_vector.insert( delegates_vector.end(), copy_vec.rbegin(),
			       copy_vec.rend() );
      assert( delegates_vector.size() - old == copy_vec.size());
    } 
  } while (!delegates_vector.empty());


  return false;
}

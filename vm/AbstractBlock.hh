/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef ABSBLOCK_HH
#define ABSBLOCK_HH

#include "ExecutionContext.hh"
#include "generic_vm_entity.hh"
#include "Ref.hh"

namespace mica {

  class Closure;
  class Task;
  class Message;
  /** Just a simple bitmask set so we can tell when a method argument
   *  has been visited.  By using int, we are limiting ourselves to
   *  64 (on a 32-bit machine) possible arguments.  This doesn't
   *  seem unreasonable, however, does it?
   */
  struct ArgumentMask {
    unsigned int marked_args;
    int dispatch_generation;

    ArgumentMask() : marked_args(0) {};

    inline void clear() { marked_args = 0; }

    inline void mark_argument( unsigned int argument_no ) {
      marked_args |= (1<<argument_no);
    }

    inline bool marked_argument( unsigned int argument_no ) const {
      return bool( marked_args & (1<<argument_no) );
    }

    inline bool marked_all_of( unsigned int arguments ) const {
      return marked_args ==( (1<<arguments) - 1 );
    }
  };

  class AbstractBlock
    : public generic_vm_entity
  {
  public:
    virtual Ref<Task> make_closure( const Ref<Message> &message, 
				  const Var &definer ) = 0;

  public:
    virtual bool isBlock() const;

    /** To mark visitations during dispatch
     */
    ArgumentMask arg_mask;

  };



}

#endif /* ABSBLOCK_HH */


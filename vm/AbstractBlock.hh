/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef ABSBLOCK_HH
#define ABSBLOCK_HH

#include "Control.hh"
#include "generic_vm_entity.hh"
#include "Ref.hh"
#include "ArgumentMask.hh"

namespace mica {

  class Frame;
  class Task;
  class Message;


  class AbstractBlock
    : public generic_vm_entity
  {
  public:
    virtual Ref<Task> make_frame( const Ref<Message> &message, 
				  const Var &definer ) = 0;

  public:
    virtual bool isBlock() const;

    /** To mark visitations during dispatch
     */
    ArgumentMask arg_mask;

  };



}

#endif /* ABSBLOCK_HH */


/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"

#include <cassert>
#include <algorithm>
#include <sstream>

#include "Data.hh"
#include "Var.hh"
#include "List.hh"
#include "String.hh"
#include "Scalar.hh"
#include "Error.hh"
#include "Exceptions.hh"
#include "GlobalSymbols.hh"
#include "Task.hh"
#include "Message.hh"
#include "NativeBlock.hh"

#include "NativeFrame.hh"

using namespace mica;
using namespace std;

/** Construction, destruction
 *
 */
NativeFrame::NativeFrame( const Ref<Message> &msg, const Var &definer,
			      const Ref<NativeBlock> &block, int pool_id )
  : AbstractFrame(msg, definer, pool_id), native_block(block)
{
}

NativeFrame::NativeFrame()
  : AbstractFrame(), native_block(0)
{
}

NativeFrame::NativeFrame( const Ref<NativeFrame> &from )
  : AbstractFrame( (AbstractFrame*)(from) ),
    native_block(from->native_block)
{
}


child_set NativeFrame::child_pointers()
{
  child_set child_p( this->AbstractFrame::child_pointers() );

  child_p.push_back( (NativeBlock*)native_block );

  return child_p;
}

mica_string NativeFrame::serialize_full() const {
  mica_string s_form( this->AbstractFrame::serialize_full() );

  s_form.append( native_block->serialize() );

  return s_form;
}


mica_string NativeFrame::traceback() const
{
  mica_string dstr(" in native code method ");
  dstr.append(on.rep());
  dstr.push_back(':');
  dstr.append(selector.tostring());
  dstr.append(" on ");
  dstr.append(self.rep());

  return dstr;
}

mica_string NativeFrame::rep() const
{
  mica_string dstr("<native frame ");
  dstr.append(self.rep());
  dstr.push_back(':');
  dstr.append(selector.tostring());
  dstr.push_back('>');

  return dstr;
}


/** Virtual machine execution
 */
void NativeFrame::resume()
{
  assert(!terminated);

  try {
    /** Invoke the function and reply with the return result.
     */
    Var ret = native_block->function( this );

    reply_return( ret ); 

    /** Done here.
     */
    terminate();  

  } catch (const Ref<Error> &err) {

    /** Construct traceback.
     */
    
    mica_string errstr(err->rep());
    errstr.append( traceback() );

    reply_raise( err, errstr );

    /** Kill the frame that sent it, since it couldn't handle it.
     */
    terminate();
  }


}



/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include "Data.hh"
#include "Var.hh"
#include "Scalar.hh"
#include "Task.hh"
#include "Block.hh"
#include "Frame.hh"
#include "Symbol.hh"
#include "Block.hh"
#include "NativeFrame.hh"

#include "Exceptions.hh"

#include "NativeBlock.hh"

using namespace mica;

NativeBlock::NativeBlock(  Var (*ifunction)( const Ref<NativeFrame> &frame),
			   const mica_string &iLibraryName,
			   const mica_string &iSymbolName )
  : function(ifunction),
    libraryName(iLibraryName),
    symbolName(iSymbolName)
{}


mica_string NativeBlock::rep() const
{
  return "{NativeBlock}";
}

mica_string NativeBlock::tostring() const
{
  throw invalid_type("cannot convert native block code to string");
}


Ref<Task> NativeBlock::make_frame( const Ref<Message> &msg, 
				     const Var &definer )
{
  /** mica blocks get a Frame.  We create a new one with all the
   *  right values copied from the message.
   */
  Ref<NativeFrame>new_frame = new (aligned) NativeFrame( msg, definer, this );

  /** Return it for scheduling.
   */
  return (Task*)new_frame;
}


mica_string NativeBlock::serialize() const
{
  mica_string s_form;

  Pack( s_form, type_identifier() );

  /** Write the library name
   */
  writeString( s_form, libraryName );

  /** Write the symbol name
   */
  writeString( s_form, symbolName );

  return s_form;
}

 

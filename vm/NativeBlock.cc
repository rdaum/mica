/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include "Data.hh"
#include "Var.hh"
#include "Scalar.hh"
#include "Task.hh"
#include "Block.hh"
#include "Closure.hh"
#include "Symbol.hh"
#include "Block.hh"
#include "NativeClosure.hh"

#include "Exceptions.hh"

#include "NativeBlock.hh"

using namespace mica;

NativeBlock::NativeBlock(  Var (*ifunction)( const Ref<NativeClosure> &closure),
			   const rope_string &iLibraryName,
			   const rope_string &iSymbolName )
  : function(ifunction),
    libraryName(iLibraryName),
    symbolName(iSymbolName)
{}


rope_string NativeBlock::rep() const
{
  return "{NativeBlock}";
}

rope_string NativeBlock::tostring() const
{
  throw invalid_type("cannot convert native block code to string");
}


Ref<Task> NativeBlock::make_closure( const Ref<Message> &msg, 
				     const Var &definer )
{
  /** mica blocks get a Closure.  We create a new one with all the
   *  right values copied from the message.
   */
  Ref<NativeClosure>new_closure = new (aligned) NativeClosure( msg, definer, this );

  /** Return it for scheduling.
   */
  return (Task*)new_closure;
}


rope_string NativeBlock::serialize() const
{
  rope_string s_form;

  Pack( s_form, type_identifier() );

  /** Write the library name
   */
  writeString( s_form, libraryName );

  /** Write the symbol name
   */
  writeString( s_form, symbolName );

  return s_form;
}

 
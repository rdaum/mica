/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef NATIVEFRAME_HH
#define NATIVEFRAME_HH

#include "AbstractFrame.hh"

namespace mica
{

  class Var;
  class NativeBlock;

  /** A NativeFrame is a Task that is responsible for the execution of a
   *  native-code method.
   */
  class NativeFrame 
    : public AbstractFrame
  {
  public:
    Type::Identifier type_identifier() const { return Type::NATIVEFRAME; }

  public:
    /** Create a frame that is explicitly for the invocation of
     *  a message.
     */
    NativeFrame( const Ref<Message> &msg, const Var &definer,
		   const Ref<NativeBlock> &block, int pool_id = -1 ) ;

    /** Copy a frame
     */
    NativeFrame( const Ref<NativeFrame> &from );

  protected:
    friend class Unserializer;
    NativeFrame();
 
  public:
    child_set child_pointers();

    mica_string serialize_full() const ;

  public:
    /** Execute native method
     */
    void resume();
   
    /** returns a traceback (no header) for this frame with an error
     */
    mica_string traceback() const;
    
    mica_string rep() const;

  public:
    /** Native block code to execute.
     */
    Ref<NativeBlock> native_block;

  };



}

#endif		/* NETWORKFRAME_HH */

#ifndef ABSTRACTFRAME_HH
#define ABSTRACTFRAME_HH

#include "Task.hh"
#include "Var.hh"
#include "Ref.hh"

namespace mica {

  class AbstractFrame
    : public Task
  {
  public:
    /** Create a frame that is explicitly for the invocation of
     *  a message.
     */
    AbstractFrame( const Ref<Message> &msg, const Var &definer,
		     int pool_id = -1 ) ;

    /** Copy a frame
     */
    AbstractFrame( const Ref<AbstractFrame> &from );

    virtual ~AbstractFrame() {};

  protected:
    friend class Scheduler;
    AbstractFrame();

  public:
    virtual child_set child_pointers();

    virtual mica_string serialize_full() const;

  protected:
    /** Prepare this frame with this message
     */
    void prepare( const Ref<Message> &msg );

  public:
    /** Execute native method
     */
    virtual void resume() = 0;
   
  public:
    /** returns a traceback (no header) for this frame with an error
     */
    virtual mica_string traceback() const = 0;

    /** Return from this frame to the previous one with this
     *  return value.
     */
    void reply_return( const Var &value );

    /** Return a message from this frame which is an exception raise.
     */
    void reply_raise( const Ref<Error> &error, mica_string traceback );

  public:
    /** Source, caller, and to are the ultimate source,
     *  last caller, and the destination of the message.
     *  On is where to get the method from, usually self,
     *  except in case of pass.
     */
    Var source;
    Var caller;
    Var self;
    Var on;

    /** The selector is always Symbol representing the
     *  name of the method to invoke.
     */
    Symbol selector;

    /** Definer is usually set to the definer of the currently running
     *  method.  This is used for view filtering on slots.
     */
    Var definer;

    /** Arguments is the list of arguments to
     *  pass to the method.
     */
    var_vector args;
    
  };

}

#endif /** ABSTRACTFRAME_HH **/

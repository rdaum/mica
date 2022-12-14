/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef MESSAGE_HH
#define MESSAGE_HH

#include <vector>

namespace mica {

#include "Var.hh"
#include "generic_vm_entity.hh"

  class Task;
  class Message;

  /** A Message is a send from one object to another which is turned
   *  into a frame by the scheduler at send time.  Note that messages
   *  are also used to encapsulate replies from one task/frame to
   *  another.
   */
  class Message 
    : public generic_vm_entity
  {
  public:
    virtual Type::Identifier type_identifier() const {
      return Type::MESSAGE;
    }

  public:
    /** Parent_Frame - the frame that spawned me.  NULL if top-message.
     */
    Ref<Task> parent_task;

    /** Index into the children list on the frame that spawned us.
     */
    unsigned int msg_id;

    /** Age and ticks are for measuring the
     *  time spent during message usage.
     */
    unsigned int age;
    unsigned int ticks;

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

    /** Arguments is the list of arguments to
     *  pass to the method.
     */
    var_vector args;

  public:
    /** create an empty message
     */
    Message();

    /** construct a message with values filled in
     *  @param parent_frame the courses we tide from
     *  @param age age of the message
     *  @param ticks number of ticks of the message
     *  @param source source attached to the message
     *  @param caller originating caller object
     *  @param to destination object
     *  @param msg the actual string message
     *  @param args the arguments
     *  @param on where the message is on
     */
    Message( Ref<Task> parent_task,
	     unsigned int id, 
	     unsigned int age, 
	     unsigned int ticks, 
	     const Var &source, 
	     const Var &caller, 
	     const Var &to,
	     const Var &on,
	     const Symbol &selector, 
	     const var_vector &args ); 
  
    /** copy a message
     *  @param caller message to copy
     */
    Message( const Message &caller );

    /** assignment operator
     *  @param caller message to assign caller
     *  @return this
     */
    Message& operator=(const Message& caller);

    /** equivalence comparison operator
     *  @param v2 right hand side of comparison
     *  @return truth value of comparison
     */
    bool operator==( const Message &v2 ) const;

    virtual void append_child_pointers( child_set &child_list );

    void finalize_object();

  public:
    /** Dispatch the message!
     */
    var_vector perform( const Ref<Frame> &parent, const Var &args );

  public:
    bool isLocal() const;

  public:
    /** is this a return?
     */
    virtual bool isReturn() const;

    /** is this a raise?
     */
    virtual bool isRaise() const;

    /** is this a halt?
     */
    virtual bool isHalt() const;

    /** is this a reply?  (one of return, raise, or halt)
     */
    virtual bool isReply() const;

    /** is this a reply to this event?
     */
    virtual bool isReplyTo( const Ref<Task> &e ) const;

  public:
    virtual mica_string typeName() const { return "Message"; }

    mica_string rep() const;

    virtual void serialize_to( serialize_buffer &s_form ) const;
  };
  
  class ReturnMessage
    : public Message
  {
  public:
    Type::Identifier type_identifier() const { return Type::RETURNMESSAGE; }

  public:
    ReturnMessage() :
      Message() 
    {
    };

    ReturnMessage( Ref<Task> parent_task, 
		   unsigned int id,
		   unsigned int age, 
		   unsigned int ticks, 
		   const Var &source, 
		   const Var &caller, const Var &to, const Var &on, 
		   const Symbol &selector, 
		   const var_vector &args )
      : Message( parent_task, id, age, ticks, source, caller, to, on, 
		 selector, args ) 
    {
    }; 
  
    /** is this a return?
     */
    bool isReturn() const {
      return true;
    }; 

    mica_string typeName() const { return "ReturnMessage"; }
  };


  class RaiseMessage
    : public Message
  {
  public:
    Type::Identifier type_identifier() const { return Type::RAISEMESSAGE; }

  public:
    mica_string trace_str;
    Ref<Error> err;

  protected:
    friend class Unserializer;
    RaiseMessage() ;

  public:
    RaiseMessage( Ref<Task> parent_task, 
		  unsigned int id,
		  unsigned int age, unsigned int ticks, 
		  const Var &source, 
		  const Var &caller, const Var &to, const Var &on, 
		  const Symbol &selector, 
		  const Ref<Error> &error, 
		  mica_string traceback );

    mica_string traceback() const {
      return trace_str;
    }

    Ref<Error> error() const {
      return err;
    }

    bool isRaise() const {
      return true;
    }; 

    void append_child_pointers( child_set &child_list );

    mica_string typeName() const { return "RaiseMessage"; }
  };

  class HaltMessage
    : public Message
  {
  public:
    Type::Identifier type_identifier() const { return Type::HALTMESSAGE; }

  public:
    HaltMessage() :
      Message() 
    {};

    HaltMessage( Ref<Task> parent_task, 
		 unsigned int id,
		 unsigned int age, unsigned int ticks, 
		 const Var &source, 
		 const Var &caller, const Var &to, const Var &on, 
		 const Symbol &selector, 
		 const var_vector &args )
      : Message( parent_task, id, age, ticks, source, caller, 
		 to, on, selector, args ) 
    {}; 
 
    bool isHalt() const {
      return true;
    }; 

    mica_string typeName() const { return "HaltMessage"; }
  };

  extern int msg_count();

}

#endif

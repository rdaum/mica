#ifndef TASK_HH
#define TASK_HH

#include <vector>

#include "Timer.hh"               // timeval
#include "Var.hh"
#include "generic_vm_entity.hh"
#include "Ref.hh"
#include "Pool.hh"  // for TID

/** The default time-to-live for a task.  If 0, the task never expires.
 */
#define TASK_TIME_TO_LIVE  5000000
#define MAX_TICKS 6553600

namespace mica {

  class Message;  

  /** A Task is a scheduled event in the mica VM Scheduler.  Each Task
   *  maintains a queue of sent-from and replied-to messages, as well
   *  as a linkage back to its parent task.
   */
  class Task
    : public generic_vm_entity
  {
  public:
    Type::Identifier type_identifier() const {
      return Type::TASK;
    }

  public:
    TID tid;
    PID pid;

  public:

    /** Create a task.
     *  @param parent the task that spawned this task
     *  @param msgid the message id that marks the message slot that
     *               created this task, and to which we are to respond.
     *  @param pool_id the pool to manage the task in.  if -1,
     *         manage it in the default pool
     */
    Task( Ref<Task> parent, unsigned int msgid,
	  int pool_id = -1 );
    
    // XXX HMM copy constructor taking a pointer?
    // Why?  Because it is not really a copy constructor so much as it
    // is a constructor.  It's a "clone" method, really.  But its 
    // behaviour must vary in each descendant.  Using a virtual method
    // would mean the return type would have to be the same for each
    // child.  Otherwise we'd need a clone_task, clone_frame, clone_etc
    // scheme, which seems as messy.  So for now, it stays. -- RWD
    Task( Ref<Task> from );
   
    virtual ~Task();

    void finalize_paged_object();

  protected:
    friend class Scheduler;

    /** Build a blank task -- used only by the unserializer.
     */
    Task();

  public:

    /** Sends a message back up to the calling task.
     *  @param msg message to send to the parent task
     */
    void reply( const Ref<Message> &msg );
    
  public:
    /** Receive a reply from one of our children.
     */
    void receive( const Ref<Message> &reply_message );
    
    /** Receive an error from self or child
     */
    virtual bool receive_exception( const Ref<Error> &err );
    
  public:
    /** A notification of an event particular to this task, i.e. a
     *  network event -- typically destined only for the top task
     *  of the call chain.  Allows queueing of network output, etc.
     */
    virtual Var notify( const Var &argument );
    
    /** Receive notification of the attachment of this task to
     *  an object.
     */
    virtual void attachment( const Var &object );
    
    /** Receive notification of the detachment of this task 
     *  from an object.
     */
    virtual void detachment( const Var &object );
    
  public:
    /** Terminate this task
     */
    virtual void terminate();
    
    /** Continue executing this task
     */
    virtual void resume();
    
    /** Return whether this task has been terminated.
     */
    virtual bool is_terminated() ;
    
    /** increment ticks and raise error if max_ticks reached
     */
    void tick();
    
  private:
    bool blocked_on( unsigned int msg_id );
    void block_on( unsigned int msg_id );
    void unblock_on( unsigned int msg_id );
    
  public:
    /** Queue a message send.
     */
    Var send( const Var &source, const Var &from, const Var &to, 
	      const Var &on, const Symbol &selector,
	      const var_vector &args );
    
    /** Return a blank, blocking message for the purpose of spawning
     *  some immediate child of this task (i.e. for a lambda call)
     */
    Ref<Message> spawn();
    
  public:
    /** Activate this task, look for things to do.
     *  @return true if task is finished running and can be descheduled.
     */
    bool activate();
    
    /** Process incoming (network, etc.) events for this frame.
     */
    virtual void spool();
    
    /** Handle a received message from one of our children.
     */
    virtual void handle_message( const Ref<Message> &message );
    
    /** Cleanup after all messages have been processed.
     */
    virtual void finish_receive();


  public:
    virtual child_set child_pointers();

    virtual mica_string typeName() const { return "Message"; }

    virtual mica_string rep() const;
      
    mica_string serialize() const;
    virtual mica_string serialize_full() const;

  public:
    friend class Message;
    friend class Unserializer;

    Ref<Task> parent_task;
    unsigned int msg_id;
    
    unsigned int age;      // task age
    unsigned int ticks;    // task ticks

    unsigned long time_to_live;

    Timer expire_timer;    // When timer exceeds time_to_live, terminate
                            // this task.

    bool terminated;
    unsigned int blocked;  // bitmask of what we're blocked on.
    bool suspended;

    // by the scheduler
    std::vector<Ref<Message> > children;       // child messages
    
  };
  extern int task_count();   // Return how many active tasks there are.

}

#endif

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef SCHEDULER_HH
#define SCHEDULER_HH

#include "common/mica.h"
#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
#  include <ext/hash_map>
#else
#  include <hash_map>
#endif

#include <list>
#include <vector>

#include "hash.hh"

#include "Timer.hh"


namespace mica {

  // sync pools every 120 seconds
  #define POOL_SYNC_INTERVAL 120

  class Var;
  class Task;

  /** Scheduler is the public interface to the virtual machine and the
   *  facility for sharing the virtual machine's time.
   *
   *  Tasks are cycled through and activated until they are
   *  finished executing or have been killed off.
   */
  class Scheduler
  {
  private:
    /** Queue of suspended threads.
     */
    std::list<Ref<Task> > eventQueue;

    /** Mapping of objects to tasks to receive notifications
     */
    typedef STD_EXT_NS::hash_map<Var, Ref<Task>, hash_var> VarTaskMap;
    VarTaskMap notifiers;

    /** Set to true at start, shutdown flips it
     */
    bool running;

  public:
    child_set child_pointers();

  public:
    Scheduler();

    ~Scheduler();

  public:
    /** Singleton's static global instance.
     */
    static Scheduler *instance;

    static void initialize();

  public:
    void start();

    void stop();

    /** The main loop, return true if the scheduler is still running.
     */
    bool run();

  private:
    /** Process the events queue.  Return the number of tasks
     *  handled.
     */
    size_t processTasks();

  public:

    /** For sending a non-blocking message without a task to return to.
     */
    void send_nonblock( size_t age, size_t ticks,
			const Var &source, 
			const Var &from,
			const Var &to, 
			const Var &on,
			const Symbol &selector,
			const var_vector &args );
    

  public:
    /** Attach an object to a task, causing the task to be notified
     *  when a method on that object executes the NOTIFY opcode.  This
     *  allows for dispatching of outgoing network events, among other
     *  things.
     */
    void attach( const Var &who, const Ref<Task> &task );

    /** Detach an object (finds its task)
     */
    void detach( const Var &who );

    /** Detach a task completely
     */
    void detach( const Ref<Task> &task );


    /** Notify a task associated with an object of a particular occurence.
     */
    Var notify( const Var &who, const Var &what );

  public:
    /** Add an event to the queue
     */
    void event_add( const Ref<Task> &e );

    void event_rm( const Ref<Task> &e );

    var_vector tasks() const;

    bool has_task( const Ref<Task>& e) const;

  public:
    void shutdown();


  };
}

#endif

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <cassert>

#ifdef _WIN32
#include <sys/types.h>
#include <sys/timeb.h>
#endif

#include <sstream>

#include "Data.hh"
#include "Var.hh"
#include "Scalar.hh"
#include "Error.hh"
#include "Symbol.hh"
#include "Exceptions.hh"

#include "Closure.hh"

#include "Scheduler.hh"
#include "Message.hh"
#include "Task.hh"
#include "Pools.hh"

#include "Timer.hh"

#include "logging.hh"

using namespace mica;
using namespace std;

static int task_counter = 0;

Task::Task()
  : generic_vm_entity(), parent_task(0)
{
  paged = true;
  task_counter++;

  expire_timer.reset();

}

Task::Task( Ref<Task> parent, size_t msgid, int pool_id )
  : generic_vm_entity(),
    parent_task(parent)
{
  time_to_live = TASK_TIME_TO_LIVE;

  task_counter++;
  paged = true;

  expire_timer.reset();

  terminated = false;
  blocked = 0;

  children.clear();
  msg_id = msgid;

  age = 0;
  ticks = 0;

  if (pool_id == -1) 
    pid = Pools::instance.getDefault();
  else
    pid = pool_id;
  
  Pool *pool = Pools::instance.get(pid);
  tid = pool->manage_task( this );
}

Task::Task( Ref<Task>from )
  : generic_vm_entity(),
    pid(from->pid),
    parent_task(from->parent_task),
    msg_id(from->msg_id),
    age(from->age),
    ticks(from->ticks),
    time_to_live(from->time_to_live),
    expire_timer(from->expire_timer),
    terminated(from->terminated),
    blocked(from->blocked),
    children(from->children)
{
  task_counter++;
  paged = true;

  Pool *pool = Pools::instance.get(pid);
  tid = pool->manage_task( this );

  expire_timer.reset();
}

Task::~Task() {}

void Task::finalize_paged_object() {
  task_counter--;
  cerr << "Collecting task: " << tid << endl;

  Pool *pool = Pools::instance.get(pid);  
  pool->unmanage_task( tid );

}

int mica::task_count()
{
  return task_counter;
}

void Task::tick() {
  ticks++;
  if (ticks >= MAX_TICKS ) {
    throw max_ticks("maximum ticks");
  }
}

void Task::reply( const Ref<Message> &message ) {

  // Make sure that the replying message is marked with the correct
  // message id.
  assert( message->msg_id == msg_id );

  if ((Task*)parent_task != 0) {

    parent_task->receive( message );

    /** Break the link with the parent, so that we can be collected
     */
    parent_task = 0;

  }
  else {
    logger.errorStream() << "attempt to send a reply from top-level task.  pid: " << pid << " tid: " << tid << log4cpp::CategoryStream::ENDLINE;
  }
}

void Task::block_on( unsigned int msg_id ) {

  assert( msg_id < 32 );

  unsigned int block_mask = (1<<msg_id);

  /** Make sure we're not blocked on this already
   */
  assert( !(blocked & block_mask) );

  blocked |= block_mask;
}

void Task::unblock_on( unsigned int msg_id ) {

  assert( msg_id < 32 );

  unsigned int block_mask = (1<<msg_id);

  /** Make sure we're blocked on this 
   */
  assert( (blocked & block_mask) );

  blocked ^= block_mask;
}

bool Task::blocked_on( unsigned int msg_id ) {
  assert( msg_id < 32 );

  unsigned int block_mask = (1<<msg_id);

  return blocked & block_mask;
}

void Task::receive( const Ref<Message> &msg ) {

  if (!terminated) {

    /** Just to verify we're receiving the right messages
     */
    assert(msg->msg_id < children.size());
    assert(children[msg->msg_id]->msg_id == msg->msg_id);

    /** If we're not blocked, we shouldn't be receiving this
     */
    assert(blocked_on(msg->msg_id));

    /** Swap the message.  Unblock
     */
    children[msg->msg_id] = msg; 
    
    unblock_on( msg->msg_id );

  } else {
    
    logger.errorStream() << "reply was sent to a terminated task.  pid: " << pid << " tid: " << tid << log4cpp::CategoryStream::ENDLINE;

  }

}

bool Task::receive_exception( const Var &err ) {
  return false;
}

Var Task::notify( const Var &argument ) {
  return NONE;
}

void Task::attachment( const Var &object ) {
}

void Task::detachment( const Var &object ) {
}

void Task::terminate() {
  terminated = true;
}

bool Task::is_terminated()  {
  return terminated;
}


void Task::resume() {
}

Var Task::send( const Var &source, const Var &from, const Var &to, 
		const Var &on, const Symbol &selector,
		const var_vector &args ) {

  unsigned int msg_id = children.size();

  Ref<Message> msg(new (aligned) Message( this, msg_id, age + 1, ticks,
				source, from, to, on, selector, args ));

  children.push_back( msg );

  /** Block resumption until the message is replied to.
   */
  block_on( msg_id );

  return Var(msg);  
}


Ref<Message> Task::spawn() {
  /** Put a dummy message for blocking in here
   */
  Ref<Message> msg = new (aligned) Message();

  msg->msg_id = children.size();
  children.push_back( msg );

  block_on( msg->msg_id );

  msg->parent_task = Ref<Task>(this);

  return msg;
}

bool Task::activate() {

  /** Check for termination before doing anything else.
   */
  if (is_terminated())
    return true;

  /** Reset the expire timer
   */ 
  if (!expire_timer.started)
    expire_timer.reset();

  /** Receive any incoming new messages.
   */
  spool();
   
  /** Look at all non-blocking, reply messages.
   */
  for (vector<Ref<Message> >::iterator ci = children.begin(); 
       ci != children.end(); ci++) {

    Ref<Message> child_message(*ci);

    /** Unblocked?  Yes, it's a reply.  Process.
     */
    if ((Message*)child_message != 0) {

      if (!blocked_on( child_message->msg_id )) {
	ticks = child_message->ticks;

	*ci = Ref<Message>(0);

	handle_message( child_message );
      } 
    }
  }

  /** We're blocked, so we're waiting until some other method finishes.
   */
  if (!blocked) {

    /** Not blocked, so that means we can clear out the children
     *  vector and continue executing.
     */
    finish_receive();

    /** Don't bother continuing if we're terminated.
     */
    if (!terminated) {
      resume();
    }
  }

  return terminated;
}

void Task::spool() {
}

void Task::handle_message( const Ref<Message> &message ) {

}

void Task::finish_receive() {

  if (children.size())
    children.clear();

}


child_set Task::child_pointers() {
  child_set child_p;

  for (vector<Ref<Message> >::iterator x = children.begin(); 
       x != children.end(); x++) {
    Message *msg = (Message*)*x;
    child_p.push_back(msg);
  }
  
  if ((Task*)parent_task != 0) {
    child_p.push_back( (Task*)parent_task );
  }

  return child_p;
}


rope_string Task::rep() const {
  rope_string dstr("<task pid: ");
  char identstr[80];
  snprintf( identstr, 80, "pid: %d %tid: %d msg_id %d",
	    pid, tid, msg_id );
  dstr.append(identstr);
  dstr.push_back('>');
  return dstr;
}


/** This is the weak serialize form -- serializes a reference to the
 *  task, not the actual task.  For that, you need serialize_full,
 *  and only PersistentPool can call that.
 */
rope_string Task::serialize() const {
  rope_string s_form;

  Pack( s_form, Type::TASK_HANDLE );  // It's not really a task, so don't lie.
  
  /** Serialize the handle information
   */
  s_form.append( Pools::instance.get(pid)->poolName.serialize() );

  Pack( s_form, tid );

  return s_form;
}

/** Full serialize method, invoked by the PersistentPool only.
 */
rope_string Task::serialize_full() const {
  rope_string s_form;

  Pack( s_form, type_identifier() );

  /** Serialize the reference count
   */
  Pack( s_form, refcnt );

  /** Serialize the handle information
   */
  s_form.append( Pools::instance.get(pid)->poolName.serialize() );

  Pack( s_form, tid );

  /** Now all the magic parts
   */
  bool exists( (Task*)parent_task );
  Pack( s_form, exists );
  if (exists)
    s_form.append( parent_task->serialize() );


  Pack( s_form, msg_id );
  Pack( s_form, age );
  Pack( s_form, ticks );
  Pack( s_form, time_to_live );
  Pack( s_form, expire_timer );
  Pack( s_form, terminated );
  Pack( s_form, blocked );
  
  Pack( s_form, children.size() );
  for (vector<Ref<Message> >::const_iterator x = children.begin();
       x != children.end(); x++) {
    s_form.append( (*x)->serialize() );
  }

  return s_form;
}

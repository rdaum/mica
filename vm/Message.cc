/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include "Data.hh"
#include "Var.hh"
#include "Scalar.hh"
#include "NoReturn.hh"
#include "Task.hh"
#include "Closure.hh"
#include "Symbol.hh"

#include "Block.hh"
#include "Exceptions.hh"
#include "Scheduler.hh"
#include "GlobalSymbols.hh"
#include "String.hh"
#include "Message.hh"
#include "Slots.hh"

using namespace mica;
using namespace std;

static int msg_counter = 0;

Message::Message()
  : generic_vm_entity(),
    parent_task(0),
    selector(NONE_SYM)
{
  msg_counter++;
  age = ticks = 0;
  source = caller = self = Var();
  args.clear();
}


Message::Message( Ref<Task>anc, size_t id, 
		  size_t Iage, size_t Iticks, 
		  const Var &Isource,
		  const Var &Icaller, 
		  const Var &Iself,
		  const Var &on,
		  const Symbol &Iselector, 
		  const var_vector &Iargs )
  : generic_vm_entity(),
    parent_task(anc),
    msg_id(id),
    age(Iage),
    ticks(Iticks),
    source(Isource),
    caller(Icaller),
    self(Iself),
    on(on),
    selector(Iselector),
    args(Iargs)
{
  msg_counter++;
}

Message::Message( const Message &caller )
  : generic_vm_entity(), 
    parent_task(caller.parent_task),
    msg_id(caller.msg_id),
    age(caller.age),
    ticks(caller.ticks),
    source(caller.source),
    caller(caller.caller),
    self(caller.self),
    on(caller.on),
    selector(caller.selector),
    args(caller.args)
{
  msg_counter++;
}

 
Message& Message::operator=(const Message& f)
{
  if (this == &f)
    return *this;

  parent_task = f.parent_task;
  msg_id = f.msg_id;
  age = f.age;
  ticks = f.ticks;
  source = f.source;
  caller = f.caller;
  self = f.self;
  on = f.on;
  selector = f.selector;
  
  args = f.args;

  return *this;
}



bool Message::operator==( const Message &v2 ) const
{
  return ( ((Task*)parent_task) == ((Task*)v2.parent_task)
	   && msg_id == v2.msg_id );
}

 
/** is this a reply?
 */
bool Message::isReply() const
{
   return ( isReturn() || isRaise() || isHalt() );
}

/** is this a return?
 */
bool Message::isReturn() const
{
  return false;
}


/** is this a raise?
 */
bool Message::isRaise() const
{
  return false;
}


/** is this a halt?
 */
bool Message::isHalt() const
{
  return false;
}

bool Message::isReplyTo( const Ref<Task> &e ) const
{
  return ((Task*)parent_task == (Task*)e);
}

child_set Message::child_pointers() {

  child_set children(data_list(args));

  children << source << caller << self << on;

  if ((Task*)parent_task != 0)    
    children.push_back( (Task*)parent_task );

  return children; 
}

bool Message::isLocal() const
{
  return true;
}

void Message::finalize_object()
{
  msg_counter--;
}

Var Message::perform( const Ref<Task> &parent, const Var &args )
{
  /** Resolve the selector on the object.
   */
  SlotResult slot_result( Slots::match_verb( on, selector, this->args ) );

  /** Make sure it's a block
   */
  if (!slot_result.value.isBlock())
    throw invalid_type("invalid block for message send");

  /** Ask the block for a task (a closure of some sort) for executing
   *  this method.
   */
  Ref<AbstractBlock> block(slot_result.value->asRef<AbstractBlock>());
  Ref<Task> task(block->make_closure( Ref<Message>(this), 
				      slot_result.definer ));

  /** Schedule the task
   */
  Scheduler::instance->event_add( task );

  return NoReturn::instance;
}


rope_string Message::rep() const
{
  rope_string rep = self.rep();
  rep.push_back( ':' );
  rep.append( selector.tostring() );
  rep.append( "( " );
  var_vector::const_iterator si;
  for (si = args.begin(); si != args.end();) {
    rep.append( si->rep() );
    si++;
    if (si == args.end())
      break;
    else
      rep.append( ", " );
  }
  rep.append(" )");

  return rep;
}

rope_string Message::serialize() const
{
  rope_string s_form;

  Pack( s_form, type_identifier() );

  /** Serialize the task (this serializes a reference to the task,
   *  not the task itself.
   */
  bool exists = (Task*)parent_task;
  Pack( s_form, exists );
  if (exists)
    s_form.append( parent_task->serialize() );

  Pack( s_form, msg_id );
  Pack( s_form, age );
  Pack( s_form, ticks );

  s_form.append( source.serialize() );
  s_form.append( caller.serialize() );
  s_form.append( self.serialize() );
  s_form.append( on.serialize() );

  s_form.append( selector.serialize() );

  Pack( s_form, args.size() );

  var_vector::const_iterator x;
  for (x = args.begin(); x != args.end(); x++)
    s_form.append( x->serialize() );

  return s_form;
}

RaiseMessage::RaiseMessage( Ref<Task> parent_task, 
			    unsigned int id,
			    unsigned int age, unsigned int ticks, 
			    const Var &source, 
			    const Var &caller, const Var &to, const Var &on, 
			    const Symbol &selector, 
			    const Ref<Error> &error, 
			    rope_string traceback )
  : Message( parent_task, id, age, ticks, source, caller, to, on, 
	     selector, var_vector() ),
    trace_str( traceback ), err(error)
{    
}; 

child_set RaiseMessage::child_pointers() {
  child_set children( this->Message::child_pointers() );
  if ((Error*)err)
    children.push_back( (Error*)err );

  return children;
}

RaiseMessage::RaiseMessage() 
  : Message(),
    err(Ref<Error>(0)) {}

int mica::msg_count()
{
  return msg_counter;
}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"

#include "Data.hh"
#include "Var.hh"
#include "Task.hh"
#include "Symbol.hh"
#include "List.hh"
#include "Scheduler.hh"
#include "String.hh"
#include "Scalar.hh"
#include "Error.hh"
#include "NoReturn.hh"
#include "Exceptions.hh"
#include "GlobalSymbols.hh"
#include "Message.hh"

#include "AbstractFrame.hh"

using namespace mica;
using namespace std;

/** Construction, destruction
 *
 */
AbstractFrame::AbstractFrame( const Ref<Message> &msg, 
				  const Var &definer_scope,
				  int pool_id )
  : Task( msg->parent_task, msg->msg_id, pool_id ), selector(NONE_SYM),
    definer(definer_scope)
{
  /** Just copy what we need from the message
   */
  prepare( msg );
}

AbstractFrame::AbstractFrame()
  : Task(), selector()
{}

AbstractFrame::AbstractFrame( const Ref<AbstractFrame> &from )
  : Task((Task*)from),
    source(from->source), caller(from->caller),
    self(from->self), on(from->on),
    selector(from->selector), definer(from->definer), args(from->args)
{
}

void AbstractFrame::prepare( const Ref<Message> &msg )
{
  /** Copy all the context from the message.
   */
  age = msg->age;
  source = msg->source;
  caller = msg->caller;
  self = msg->self;
  selector = msg->selector;
  args = msg->args;

}



child_set AbstractFrame::child_pointers()
{
  child_set child_p(this->Task::child_pointers());

  child_p << self << caller << source << definer << on;

  append_datas( child_p, args );

  return child_p;
}

void AbstractFrame::reply_return( const Var &value )
{
  var_vector arguments;
  arguments.push_back(value);
  reply( new (aligned) ReturnMessage( this, msg_id, age, ticks, source, caller,
				      self, on, selector, arguments ) );

}

void AbstractFrame::reply_raise( const Ref<Error> &error, 
				   mica_string traceback ) {
  reply( new (aligned) RaiseMessage( this, msg_id, age, ticks, source, caller,
				     self, on, selector, error, traceback ) );
}

mica_string AbstractFrame::serialize_full() const
{
  mica_string s_form( this->Task::serialize_full() );

  s_form.append( source.serialize() );
  s_form.append( caller.serialize() );
  s_form.append( self.serialize() );
  s_form.append( on.serialize() );

  s_form.append( selector.serialize() );

  s_form.append( definer.serialize() );

  Pack( s_form, args.size() );

  var_vector::const_iterator x;
  for (x = args.begin(); x != args.end(); x++)
    s_form.append( x->serialize() );

  return s_form;

}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "vm/Scheduler.hh"

#include <cassert>
#include <ctime>
#include <utility>
#include <iostream>

#include "common/mica.h"
#include "types/Atom.hh"
#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/List.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "types/Workspace.hh"
#include "vm/Block.hh"
#include "vm/Frame.hh"
#include "vm/Message.hh"
#include "vm/Task.hh"

using namespace std;
using namespace mica;

Scheduler *Scheduler::instance;

void Scheduler::initialize() { Scheduler::instance = new (aligned) Scheduler(); }

Scheduler::Scheduler() {
  notifiers.clear();
  eventQueue.clear();
}

/** IF the scheduler is going down, one may safely assume that all
 *  the queued events and messages should be cleaned up as well.
 */
Scheduler::~Scheduler() { eventQueue.clear(); }

size_t Scheduler::processTasks() {
  unsigned int events_handled = 0;

  /** take a pass through the eventQueue, and attempt to process
   *  each event.  Leave behind any events which are a) blocked
   *  or b) added during iteration.  Check for task timeouts and
   *  terminate them if they've gone on too long.
   */
  if (!eventQueue.empty())
    for (std::list<Ref<Task> >::iterator cur_task_iterator = eventQueue.begin();
         cur_task_iterator != eventQueue.end();) {
      Ref<Task> cur_task = *cur_task_iterator;

      /** attempt to activate a task
       */
      if (cur_task->activate()) {
        /** Store an iterator pointing to the next task in the queue.
         *  This way we can erase the cur task and continue on
         *  with the next one.
         */
        std::list<Ref<Task> >::iterator next_task_iterator = cur_task_iterator;
        next_task_iterator++;

        /** Remove this task from the queue
         */
        eventQueue.erase(cur_task_iterator);

        /** Count this task as finished
         */
        events_handled++;

        /** Onward, forwards.
         */
        cur_task_iterator = next_task_iterator;

      } else {
        /** Check task for timeout.  If it's timed out, kill it off and
         *  raise an error to its parent.
         */
        if (cur_task->time_to_live && (cur_task->expire_timer.elapsed() > cur_task->time_to_live)) {
          cur_task->terminate();
        }

        /** Move along
         */
        cur_task_iterator++;
      }
    }

  return events_handled;
}

void Scheduler::stop() { running = false; }

void Scheduler::start() { running = true; }

bool Scheduler::run() {
  /** process events and process messages while there are still
   *  events and messages that can be processed.
   */
  static int processed = 0;
  bool first = true;

  while (running) {
    int old_processed = processed;
    processed = processTasks();
    if (processed && first)
      first = false;

    /** Every time we have processed no events but have processed
     *  some recently (i.e. there's a "pause") we can invoke the
     *  cycle collector and then return to the select (or other poll)
     *
     *  This really needs to be investigated.   The reason we are doing
     *  this is because collecting cycles at the "wrong time" (undefined)
     *  seems to yield segfaults/invalid-data, all sorts of ugliness.
     *  I would like to know why.
     *
     *  Todo: sync pools every X seconds
     */
    if (processed == 0 && old_processed == 0 && !first) {
      reference_counted::collect_cycles();
      break;
    }
  }

  return running;
}

void Scheduler::attach(const Var &who, const Ref<Task> &task) {
  if (!who.isData())
    throw internal_error("cannot attach a non-Data type");

  notifiers.insert(make_pair(who, task));

  task->attachment(who);
}

void Scheduler::detach(const Ref<Task> &task) {
  VarTaskMap::iterator fi;
  for (fi = notifiers.begin(); fi != notifiers.end(); fi++) {
    if (fi->second == task) {
      fi->second->detachment(fi->first);
      notifiers.erase(fi);
      return;
    }
  }
  throw internal_error("task is not registered for notifications");
}

void Scheduler::detach(const Var &who) {
  VarTaskMap::iterator fi = notifiers.find(who);
  if (fi == notifiers.end())
    throw internal_error("cannot match object to task");

  fi->second->detachment(who);
  notifiers.erase(fi);
}

/** Some optimization needed here- hash_map top level tasks to
 *  objects, probably.
 */
Var Scheduler::notify(const Var &who, const Var &what) {
  VarTaskMap::iterator fi = notifiers.find(who);
  if (fi == notifiers.end())
    throw internal_error("cannot match object to task");

  return fi->second->notify(what);
}

void Scheduler::send_nonblock(size_t age, size_t ticks, const Var &source, const Var &from,
                              const Var &to, const Var &on, const Symbol &selector,
                              const var_vector &args) {
  Var msg =
      new (aligned) Message((Ref<Task>)0, 0, age, ticks, source, from, to, on, selector, args);

  /** Dispatch the message now.
   */
  msg.perform(((Ref<Frame>)0), NONE);
}

/*
 * Add an event to the event queue.
 */
void Scheduler::event_add(const Ref<Task> &e) { eventQueue.push_back(e); }

/*
 * Check for the existence of a task in the queue
 */
bool Scheduler::has_task(const Ref<Task> &e) const {
  for (list<Ref<Task> >::const_iterator x = eventQueue.begin(); x != eventQueue.end(); x++)
    if (*x == e)
      return true;

  return false;
}

/*
 * Add an event to the event queue.
 */
void Scheduler::event_rm(const Ref<Task> &e) {
  for (list<Ref<Task> >::iterator x = eventQueue.begin(); x != eventQueue.end(); x++)
    if (*x == e) {
      eventQueue.erase(x);
      break;
    }
}

var_vector Scheduler::tasks() const {
  var_vector task_vector;

  for (list<Ref<Task> >::const_iterator x = eventQueue.begin(); x != eventQueue.end(); x++)
    task_vector.push_back(Var(*x));

  return task_vector;
}

void Scheduler::shutdown() {
  running = false;

  delete Scheduler::instance;
  instance = 0;
}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */

#if defined(_WIN32)
#error mica flattenole not ported to win32 [XXX FIXME]
#endif



#include <iostream>
#include <vector>
#include <sstream>

#include <sys/types.h>
#include <sys/time.h>	
#include <sys/socket.h>	

#include "Data.hh"
#include "Var.hh"
#include "Object.hh"

#include "Exceptions.hh"
#include "Scheduler.hh"
#include "Task.hh"

#include "Symbol.hh"
#include "Message.hh"
#include "Block.hh"
#include "OpCodes.hh"
#include "MetaObjects.hh"
#include "Error.hh"
#include "MicaParser.hh"
#include "Slots.hh"

#include "Frame.hh"

#include "Pool.hh"
#include "Pools.hh"
#include "PersistentPool.hh"
#include "GlobalSymbols.hh"
#include "List.hh"

#include "compile.hh"
 
#include <readline/readline.h>
#include <readline/history.h>

#include "logging.hh"

#define DEBUG_OPCODES

using namespace mica;
using namespace std;

std::vector<mica_string> stringStack;

static int verbose;


class EvalLoopTask
  : public Task
{
private:
  Var eval_obj;

public:
  
  EvalLoopTask( const Var &ieval_obj )
    : Task(0,0), eval_obj(ieval_obj)
  {
    time_to_live = 0;
    Scheduler::instance->attach( eval_obj, this );

    logger.infoStream() << "created evaluation task" << log4cpp::CategoryStream::ENDLINE;
  };
  

  child_set child_pointers() {
    child_set child_p(this->Task::child_pointers());
    child_p << eval_obj;
    return child_p;
  }

  void finalize_object() {

    logger.infoStream() << "deleted evaluation task" << log4cpp::CategoryStream::ENDLINE;
    this->Task::finalize_object();
  }

  void spool() {
    
    if (!stringStack.size())
      return;

      /** Compile it to 'eval'
       */
      mica_string code(stringStack.back());
      stringStack.pop_back();

      var_vector args;
      
      Ref<Block> block( mica::compile(code) );

#ifdef DEBUG_OPCODES
      cout << block->rep() << endl;
#endif
      
      Slots::assign_verb( eval_obj, EVAL_TMP_SYM, var_vector(), Var(block) );
      
      Var msg = send( MetaObjects::SystemMeta, MetaObjects::SystemMeta,
		      eval_obj, eval_obj, 
		      EVAL_TMP_SYM, args );
      msg.perform( Ref<Frame>(0), NONE );
  };
  

  Var notify( const Var &argument ) {
    cout << "NOTIFY: " << argument << endl;
    return Var();
  }

  mica_string rep() const
  {
    std::ostringstream dstr;
    
    dstr << "<eval_loop " << this << ">";
    
#ifndef OSTRSTREAM_APPENDS_NULLS
    dstr << std::ends;
#endif
    
    return dstr.str().c_str();
  }

  void handle_message( const Ref<Message> &reply_message )
  {
    if (reply_message->isReturn()) {
      cout << "=> " << reply_message->args[0] << endl;
    } else if (reply_message->isRaise()) {

      cerr<< reply_message->asRef<RaiseMessage>()->traceback() << endl;
      
    } else if (reply_message->isHalt()) {
      cout << "Halted." << endl;
    } 

  }
};



void evalLoop( const Var &eval_obj ) {
  Scheduler::instance->start();

  /** Build a top-level frame for our session.  Schedule it, then
   *  we can send messages from it.
   */
  Task *eval_task = new (aligned) EvalLoopTask( eval_obj );

  Scheduler::instance->event_add( eval_task );
  
  do {
  read:
    char *line = readline("mica> ");
    if (line) {
      add_history(line);
    
      try {
	stringStack.push_back( line );
	eval_task->spool();
      } catch (::parse_error pe) {
	cout << "parse error in line #" << pe.line 
	     << " column #" << pe.column << endl;
	goto read;
      } catch (::lex_error le) {
	cout << "lex error in line #" << le.line 
	     << " column #" << le.column << endl;      
	goto read;
      } catch (const Ref<Error> &err) {
	cout << "Caught: " << Var(err) << endl;
	goto read;
      }

    } else {
      Scheduler::instance->stop();
    }
  } while (Scheduler::instance->run());

  Scheduler::instance->detach( Ref<Task>(eval_task) );
  Scheduler::instance->event_rm( eval_task );

  reference_counted::collect_cycles();

  Scheduler::instance->shutdown();
}


int main( int argc, char *argv[] )
{
  verbose = 2;		// XXX FIXME use getopt for --verbose

  Scheduler::initialize();
  initializeOpcodes();
  initialize_log( true );
  
  Pool *default_pool = 0;

  try {
    logger.infoStream() << "initializing symbols" << log4cpp::CategoryStream::ENDLINE;
    initSymbols();
    
    logger.infoStream() << "opening builtin pool" << log4cpp::CategoryStream::ENDLINE;   
    pair<PID, Var> pool_return = Pool::open( Symbol::create("builtin") ); 
    Pools::instance.setDefault( pool_return.first );

    logger.infoStream() << "initializing builtins" << log4cpp::CategoryStream::ENDLINE;
    MetaObjects::initialize( pool_return.second );

    default_pool = Pools::instance.get(pool_return.first);

    //    initNatives();

  } catch (Ref<Error> e) {
    cerr << e << endl;
    exit(-1);
  }

  /** Do an initial cycle collection.
   */
  reference_counted::collect_cycles();

  int pool_c = 0;
  for (pool_c = 0; pool_c < argc-1; pool_c++) {
    char *pool_name = argv[pool_c + 1];
    try {
      logger.infoStream() << "opening pool:" << pool_name << log4cpp::CategoryStream::ENDLINE;

      pair<PID, Var> p_pool_return( PersistentPool::open( Symbol::create(pool_name), MetaObjects::Lobby->asRef<Object>() ) );
      
      Pools::instance.setDefault( p_pool_return.first );
      default_pool = Pools::instance.get( p_pool_return.first );
    } catch (Ref<Error> e) {
      logger.infoStream() << "unable to open pool:" << pool_name << log4cpp::CategoryStream::ENDLINE;
    }
  }

  Var default_lobby = Var(default_pool->lobby);
  
  /** Create :eval_tmp slot for later writing
   */
  Slots::declare_verb( default_lobby, EVAL_TMP_SYM, var_vector(), NONE );

  logger.infoStream() << "evaluating on object: " << default_lobby << log4cpp::CategoryStream::ENDLINE;

  //  populate_list_meta();

  /** Now start evaluating.
   */
  try {
    evalLoop( default_lobby );

  } catch (const Ref<Error> &e) {
    cout << "outer level: " << Var(e) << endl;
  }
		

  /** Remove :eval_tmp slot 
   */
  Slots::remove_verb( default_lobby, EVAL_TMP_SYM, var_vector() );


  /** This has to get cleaned out before closing the pool
   */
  default_lobby = NONE;


  logger.infoStream() << "closing pools" << log4cpp::CategoryStream::ENDLINE;   
  Pools::instance.close();

  logger.infoStream() << "cleaning up metaobject references" << log4cpp::CategoryStream::ENDLINE;   
  MetaObjects::cleanup();

  logger.infoStream() << "unloading DLLs" << log4cpp::CategoryStream::ENDLINE;   
  //  unloadDLLs();

  logger.infoStream() << "exiting" << log4cpp::CategoryStream::ENDLINE;   

  close_log();

  return 0;
  
}


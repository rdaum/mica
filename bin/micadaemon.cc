/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#include "common/mica.h"
#include "config.h"


#include <iostream>
#include <vector>
#include <sstream>
#include <boost/lexical_cast.hpp>

#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_map>
#else
#include <hash_map>
#endif

#include <ctype.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>	
#include <sys/socket.h>	
#include <resolv.h>
#include <errno.h>
#include <signal.h>

#include "Data.hh"
#include "Var.hh"
#include "Object.hh"

#include "Exceptions.hh"
#include "Scheduler.hh"
#include "Task.hh"

#include "Symbol.hh"
#include "Message.hh"
#include "NativeBind.hh"
#include "Block.hh"
#include "OpCodes.hh"
#include "MetaObjects.hh"
#include "Error.hh"
#include "MicaParser.hh"
#include "String.hh"
#include "Pool.hh"
#include "Pools.hh"
#include "PersistentPool.hh"
#include "GlobalSymbols.hh"
#include "List.hh"
#include "Slots.hh"
#include "hash.hh"
#include "logging.hh"

using namespace mica;
using namespace std;

static int verbose;
static Var connection_proto;


void daemon_signal_handler (int)
{
  Scheduler::instance->stop();
}

class ConnectionTask
  : public Task
{
public:
  Var connection;
  mica_string incoming_buffer;
  mica_string outgoing_buffer;
  int socket;
  bool disconnected;

public:

  ConnectionTask( const Var &conn_obj, int socket_fd )
    : Task(0,0), 
      connection(conn_obj),
      socket(socket_fd)
  {
    time_to_live = 0;
    logger.infoStream() << "created network connection task" << log4cpp::CategoryStream::ENDLINE;
    Scheduler::instance->attach( conn_obj, this );
    disconnected = false;

    try {
      var_vector args;
      Var msg = send( connection, connection, connection, connection, 
		      ATTACH_SYM, args );
      msg.perform( this, List::from_vector( args ));
    } catch (const Ref<Error> &e) {
      logger.errorStream() << "error during dispatch to :attach (" << e << ")" << log4cpp::CategoryStream::ENDLINE;
    }

  };
  
  void detachment( const Var &object ) {
    try {
      var_vector args;
      Var msg = send( connection, connection, connection, connection, 
		      DETACH_SYM, args );
      msg.perform( this, List::from_vector( args ));
    } catch (const Ref<Error> &e) {
      logger.errorStream() << "error during dispatch to :detach (" << e << ")" << log4cpp::CategoryStream::ENDLINE;
    }
      
    if (!disconnected) {
      disconnected = true;
      close( socket );
    }
  }

  child_set child_pointers() {
    child_set child_p(this->Task::child_pointers());
    child_p << connection;
    return child_p;
  }

  void finalize_object() {
    logger.infoStream() << "destroying network connection task" << log4cpp::CategoryStream::ENDLINE;
    this->Task::finalize_object();
  }
  

  Var notify( const Var &argument ) {
    outgoing_buffer.append( argument.tostring() );
    outgoing_buffer.push_back('\n');
    return NONE;
  }

  void spool() {
    while (!terminated && !disconnected) {
      
      // Is there a complete line ready for send?
      mica_string::iterator r_f;
      r_f = std::find( incoming_buffer.mutable_begin(),
		       incoming_buffer.mutable_end(),
		       '\n' );
      
      // No, return and let the task continue to spool incoming
      // characters.
      if (r_f == incoming_buffer.mutable_end())
	break;
      
      mica_string line;
      if ( (*(r_f-1)) == '\r')
	line = mica_string( incoming_buffer.mutable_begin(), r_f - 1 );
      else
	line = mica_string( incoming_buffer.mutable_begin(), r_f );
      
      incoming_buffer.erase( incoming_buffer.mutable_begin(), r_f + 1);
      
      var_vector args;
      args.push_back(String::from_rope(line));

      try {
	Var msg = send( connection, connection, connection, connection, 
			RECEIVE_SYM, args );
	msg.perform( this, List::from_vector( args ));
      } catch (const Ref<Error> &e) {
	logger.errorStream() << "error during dispatch to :receive (" << e << ")" << log4cpp::CategoryStream::ENDLINE;
      }

    }
  }


  mica_string rep() const
  {
    std::ostringstream dstr;
    
    dstr << "<connection task " << this << ">";
    
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
      mica_string traceback = reply_message->args[1].tostring();

      logger.errorStream() << "traceback on connection: " << reply_message->args[1].tostring() << log4cpp::CategoryStream::ENDLINE;
      
    } else if (reply_message->isHalt()) {
      logger.errorStream() << "child task of connection halted" << log4cpp::CategoryStream::ENDLINE;
    } 

  }
};


/** Sets a socket to non-blocking
 */
void setnonblocking(int sock)
{
  int opts = fcntl(sock,F_GETFL);
  if (opts < 0) {
    logger.critStream() << "error in fcntl(F_GETFL): " << strerror(errno) << log4cpp::CategoryStream::ENDLINE;
    return;
  }
  opts = (opts | O_NONBLOCK);
  if (fcntl(sock,F_SETFL,opts) < 0) {
    logger.critStream() << "error in fcntl(F_SETFL): " << strerror(errno) << log4cpp::CategoryStream::ENDLINE;
    return;
  }
  return;
}

/** Socket file descriptor of our listening socket
 */
static int listening_socket;

/** Map from socket descriptor to listening ConnectionTasks
 */
typedef STD_EXT_NS::hash_map<int, Ref<ConnectionTask> > ConnectionMap;
static ConnectionMap connections;  

/** Socket file descriptors we want to wake up for, using select()
 */
static fd_set read_socks;
static fd_set write_socks;

/** Highest #'d file descriptor, needed for select()
 */
static int highsock;

/**  put together fd_set for select(), which will
 *   flattenist of the sock veriable in case a new connection
 *   is coming in, plus all the sockets we have already
 *   accepted. */
void build_select_list() {

  /* FD_ZERO() clears out the fd_set called socks, so that
     it doesn't contain any file descriptors. */
	
  FD_ZERO(&read_socks);
  FD_ZERO(&write_socks);
	
  /* FD_SET() adds the file descriptor "listening_socket" to the fd_set,
     so that select() will return if a connection comes in
     on that socket (which means you have to do accept(), etc. */
	
  FD_SET(listening_socket, &read_socks);
  FD_SET(listening_socket, &write_socks);
	
  /* Loops through all the possible connections and adds
     those sockets to the fd_set */
	
  for (ConnectionMap::iterator c = connections.begin();
       c != connections.end(); c++) {
    FD_SET( c->first, &read_socks );
    FD_SET( c->first, &write_socks );
    if (c->first > highsock)
      highsock = c->first;
  }
}

void handle_new_connection() {
  /** We have a new connection coming in!
   */
  int connection = accept(listening_socket, NULL, NULL);
  if (connection < 0) {
    logger.critStream() << "error in accepting socket: " << strerror(errno) << log4cpp::CategoryStream::ENDLINE;
    return;
  }
  setnonblocking(connection);

  Ref<ConnectionTask>
    connection_task( new ConnectionTask( connection_proto, connection ) );

  connections.insert( make_pair( connection, connection_task ) );

  Scheduler::instance->event_add( (Task*)connection_task );
}

bool read_from_socket( ConnectionMap::iterator c_it ) {

  /** Read as many bytes as we can from the socket, and stick
   *  them into the buffer on the task
   */
  int actually_read;

  char buffer[512]; 
  actually_read = read( c_it->first, buffer, 512 );

  if (actually_read > 0) {
    mica_string full_buffer( buffer, actually_read );
    
    c_it->second->incoming_buffer.append( full_buffer );

  } else {

    /** Causes surrounding loop to delete us
     */
    return false;
  }

  return true;
}

bool write_to_socket( ConnectionMap::iterator c_it ) {

  /** Write many bytes as we can to the socket from the buffer
   *  in the task.
   */
  size_t buffer_size = c_it->second->outgoing_buffer.size();

  int actually_written = write( c_it->first, 
				c_it->second->outgoing_buffer.c_str(),
				buffer_size );

  if (actually_written > 0) {
    
    c_it->second->outgoing_buffer.erase( 0, actually_written );

  } else if (actually_written < 0) {


    /** Causes surrounding loop to delete us
     */
    return false;
  }

  return true;
}

void handle_socks() {
  
  /** Check for incoming connections
   */
  if (FD_ISSET(listening_socket,&read_socks))
    handle_new_connection();


  /** Now run through our sockets and see if anything happened on them
   */

  vector<ConnectionMap::iterator> to_clear;
  for (ConnectionMap::iterator c_it = connections.begin();
       c_it != connections.end(); c_it++) {
    bool cleared = false;
    if (FD_ISSET( c_it->first, &read_socks )) {
      if (!read_from_socket(c_it)) {
	cleared = true;
	to_clear.push_back(c_it);
      }
    }
    if (FD_ISSET( c_it->first, &write_socks )) {
      if (!write_to_socket(c_it) && !cleared) {
	to_clear.push_back(c_it);
      }
    }
  }
  for (vector<ConnectionMap::iterator>::iterator clear = to_clear.begin();
       clear != to_clear.end(); clear++) {
    ConnectionMap::iterator c_it = *clear;

    /** Connection closed -- remove it from the scheduler, and clear
     *  it out of the connections map
     */
    logger.infoStream() << "disconnection.  socket fd: " << c_it->first << log4cpp::CategoryStream::ENDLINE;

    if (!c_it->second->disconnected) {
      close( c_it->first );
      c_it->second->disconnected = true;
    }

    Scheduler::instance->detach( Ref<Task>((Task*)c_it->second) );

    Scheduler::instance->event_rm( (Task*)(c_it->second) );

    connections.erase(c_it);
  }
  to_clear.clear();
}

void network_loop( short port ) {

  sockaddr_in server_address; /* bind info structure */

  int reuse_addr = 1;  /* Used so we can re-bind to our port
			  while a previous connection is still
			  in TIME_WAIT state. */

  struct timeval timeout;  /* Timeout for select */

  int readsocks;	     /* Number of sockets ready for reading */

  /* Obtain a file descriptor for our "listening" socket */
  listening_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (listening_socket < 0) {
    logger.critStream() << "error in creating listening socket: " << strerror(errno) << log4cpp::CategoryStream::ENDLINE;
    return;
  }
  /* So that we can re-bind to it without TIME_WAIT problems */
  setsockopt(listening_socket, SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
	     sizeof(reuse_addr));
  
  /* Set socket to non-blocking with our setnonblocking routine */
  setnonblocking(listening_socket);
  
	
  bzero((char *) &server_address, sizeof(server_address));
  server_address.sin_family = AF_INET;
  server_address.sin_addr.s_addr = htonl(INADDR_ANY);
  server_address.sin_port = htons(port);
  if (bind(listening_socket, (sockaddr *) &server_address,
	   sizeof(server_address)) < 0 ) {
    logger.critStream() << "error in bind: " << strerror(errno) << log4cpp::CategoryStream::ENDLINE;
    close(listening_socket);
    return;
  }
  
  /* Set up queue for incoming connections. */
  listen( listening_socket, 5 );
  
  /* Since we start with only one socket, the listening socket,
     it is the highest socket so far. */
  highsock = listening_socket;

 
  Scheduler::instance->start();

  do {
    build_select_list();
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    
    readsocks = select(highsock+1, &read_socks, &write_socks, 
		       (fd_set *) 0, &timeout);
    
   
    if (readsocks < 0) {
      logger.critStream() << "error in select: " << strerror(errno) << log4cpp::CategoryStream::ENDLINE;
    } else if (readsocks)
      handle_socks();


  } while (Scheduler::instance->run());

  reference_counted::collect_cycles();

  Scheduler::instance->shutdown();
}



int main( int argc, char *argv[] )
{
  if (argc < 2) {
    cout << "Usage: " << argv[0] << " <port number> [ <pool name>, ... ]" << endl;
    exit(-1);
  }
  short port;

  try {
    port = boost::lexical_cast<short>( argv[1] );
  } catch (...) {
    cout << "Invalid port number." << endl;
    cout << "Usage: " << argv[0] << " <port number> [ <pool name>, ... ]" << endl;
    exit(-1);
  }

  Scheduler::initialize();
  initializeOpcodes();

  Pool *default_pool = 0;

  struct sigaction new_action, old_action;

  /* Set up the structure to specify the new action. */
  new_action.sa_handler = daemon_signal_handler;
  sigemptyset (&new_action.sa_mask);
  new_action.sa_flags = 0;

  sigaction (SIGINT, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGINT, &new_action, NULL);
  

  try {
    logger.infoStream() << "initializing symbols" << log4cpp::CategoryStream::ENDLINE;
    initSymbols();
    
    logger.infoStream() << "opening builtin pool" << log4cpp::CategoryStream::ENDLINE;   
    pair<PID, Var> pool_return = Pool::open( Symbol::create("builtin") ); 
    Pools::instance.setDefault( pool_return.first );

    logger.infoStream() << "initializing builtins" << log4cpp::CategoryStream::ENDLINE;
    MetaObjects::initialize( pool_return.second );

    default_pool = Pools::instance.get(pool_return.first);

    initNatives();

  } catch (Ref<Error> e) {
    cerr << e << endl;
    exit(-1);
  }

  /** Do an initial cycle collection.
   */
  reference_counted::collect_cycles();

  if (argc > 2) {
    int pool_c = 0;
    for (pool_c = 0; pool_c < argc-1; pool_c++) {
      char *pool_name = argv[pool_c + 2];
      try {
	logger.infoStream() << "opening pool:" << pool_name << log4cpp::CategoryStream::ENDLINE;

	pair<PID, Var> p_pool_return( PersistentPool::open( Symbol::create(pool_name), MetaObjects::Lobby->asRef<Object>() ) );
      
	Pools::instance.setDefault( p_pool_return.first );
	default_pool = Pools::instance.get( p_pool_return.first );
      } catch (Ref<Error> e) {
	logger.infoStream() << "unable to open pool:" << pool_name << log4cpp::CategoryStream::ENDLINE;
      }
    }
  }


  try {
    connection_proto = Slots::get_name( Var(default_pool->lobby),
					Symbol::create("connection") ).value ;
  } catch (Ref<Error> e) {
    if (e == E_SLOTNF) {
      logger.errorStream() << "default pool is missing a $connection prototype" << log4cpp::CategoryStream::ENDLINE;
      
      connection_proto = default_pool->lobby->clone();

      default_pool->lobby->declare( Var(NAME_SYM), 
				    Symbol::create("eval_obj"), 
				    connection_proto );

      logger.infoStream() << "created new connection prototype" << log4cpp::CategoryStream::ENDLINE;

    } else {
      throw;
    }

  }      

  /** Now start evaluating.
   */
  try {
    logger.infoStream()  << "starting network listener" << log4cpp::CategoryStream::ENDLINE;
    network_loop( port );
    logger.infoStream()  << "network listen finished" << log4cpp::CategoryStream::ENDLINE;
  } catch (Ref<Error> e) {
    cout << e << endl;
  }

  connection_proto = NONE;

  logger.infoStream() << "closing pools" << log4cpp::CategoryStream::ENDLINE;   
  Pools::instance.close();

  logger.infoStream() << "cleaning up metaobject references" << log4cpp::CategoryStream::ENDLINE;   
  MetaObjects::cleanup();

  logger.infoStream() << "unloading DLLs" << log4cpp::CategoryStream::ENDLINE;   
  unloadDLLs();

  logger.infoStream() << "exiting" << log4cpp::CategoryStream::ENDLINE;   

  close_log();


  return 0;  
}


/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef FRAME_HH
#define FRAME_HH

#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_map>
#else
#include <hash_map>
#endif

#include <boost/pool/pool_alloc.hpp>

#include "Task.hh"

#include "Control.hh"
#include "OpCode.hh"
#include "Var.hh"
#include "Ref.hh"
#include "Error.hh"
#include "Environment.hh"
#include "hash.hh"

namespace mica
{
  class Closure;
  typedef enum {
    CLOSURE,
    LOOP_INSIDE,
    LOOP_OUTSIDE,
    BRANCH
  } ClosureTag;

  struct ExceptionHandler {
    uint16_t var_idx;
    Ref<Closure> handler;

    ExceptionHandler( uint16_t i_var_idx, const Ref<Closure> &i_handler );
    ExceptionHandler( const ExceptionHandler &xc );
    ExceptionHandler &operator=( const ExceptionHandler &rhs );   
    bool operator==( const ExceptionHandler &rhs ) const;
    void serialize_to( serialize_buffer &buffer ) const;

  };

  typedef
  STD_EXT_NS::hash_map< Ref<Error>, ExceptionHandler,
			hash_ref, 
			std::equal_to< Ref<Error> >,
			boost::pool_allocator< Ref<Error> > > ExceptionMap;
  
  typedef enum {
      RUNNING,	  // program is running 
      STOPPED,	  // program stopped by return or end of code 
      HALTED,	  // program halted by method called 
      RAISED,	  // program stopped by raise() 
      BLOCKED,    // program blocked on event 
      SUSPENDED   // program suspended by timeout. 
    } ExecState ;

      
  /** A Frame is a kind of Task which implements the ability to execute mica
   *  VM opcodes.
   */
  class Frame 
    : public Task
  {
  public:
    Type::Identifier type_identifier() const { return Type::FRAME; }

  public:
    /** Create a frame that is explicitly for the invocation of
     *  a message.
     */
    Frame( const Ref<Message> &msg, const Var &definer,
	     const Ref<Block> program, int pool_id = -1 ) ;

    /** Copy a frame
     */
    Frame( const Ref<Frame> &from );

  protected:
    friend class Unserializer;
    Frame();
 
    struct execution_visitor {
      Frame *frame;

      execution_visitor( Frame *in_frame ) 
	: frame(in_frame) {};

      template<typename T> 
      void operator()( const T &value ) const {
	frame->push( Var(value) );
      }

      void operator()( const Op &opcode ) const {
	frame->execute_opcode( opcode );
      }
    };      

  public:
    /** Return the internal set of object pointers for cycle detection
     */
    void append_child_pointers( child_set &child_list );

    void serialize_full_to( serialize_buffer &s_form ) const;
 
    mica_string rep() const;

  public:
    /** Handle an incoming message
     */
    void handle_message( const Ref<Message> &reply_message );

    bool is_terminated() ;

  public:
    /** Execute opcodes
     */
    void execute();

    /** Resume VM.
     */
    void resume();
   
  public:
    /** Put the frame into a running state
     */
    void run();

    /** Put the frame into a stopped state.
     */
    void stop();
    
    /** Put the framene into a halted state.
     */
    void halt();

    /** returns a traceback (no header) for this frame with an error
     */
    mica_string traceback() const;

  public:
    /** pop from stack
     */
    Var pop();

    /** push to stack
     *  @param v what to push onto the exec stack
     */
    void push( const Var &v );
 
    /** pop from exec stack
     */
    Var pop_exec();

    /** push to exec stack
     *  @param v what to push onto the exec stack
     */
    void push_exec( const Var &v );

    /** Get next operation to execute
     */
    Var next();

  public:
    bool handle_exception( const Ref<Error> &err );

    /** Continue after an exception from a child.
     */
    void resume_raise( const Ref<Error> &err, mica_string traceback );

    /** Raise an exception
     */
    void raise( const Ref<Error> &err );

  public:
    void reply_return( const Var &value );

    void reply_raise( const Ref<Error> &error, 
		      mica_string traceback );

  public:

  private:
    Ref<Closure> make_closure( ClosureTag tag = CLOSURE ) const;

    void load_closure( const Ref<Closure> &closure );

    /** push an entry to the dump stack
     */
    void push_dump( const Ref<Closure> &entry );

  public:
    /** Store current program in the branch stack and load a new
     *  block as current program.
     */
    void switch_branch( const Ref<Block> &switch_to );

  public:

    /** Store current program in the loop stack and enter a new
     *  block as current program.
     */
    void loop_begin( const Ref<Block> &loop_expr );

    /** Pop the loop stack
     */
    void loop_break();

    /** Continue the current loop, pushing value to the stack
     */
    void loop_continue();

  protected:

    /** Prepare this frame with this message
     */
    void prepare( const Ref<Message> &msg );

  public:

    /** Load a closure into this frame and apply arguments to it.
     */
    void apply_closure( const Ref<Closure> &closure, const Var &args );

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
  
  public:
    const execution_visitor executor;  // visitor used to handle opcodes

    /////////////////////////////////////////////////////////////////
    var_vector stack;        	       // S - value stack

    Environment scope;	               // E - the variable environment

    ExceptionMap exceptions;           // X - exceptions

    Control control;		       // C - control

    std::vector< Ref<Closure> > dump;  // D - the closure stack
    /////////////////////////////////////////////////////////////////

    ExecState ex_state;

  public:
    void op_cdr( unsigned int, unsigned int ),
      op_car( unsigned int, unsigned int ),
      op_cons( unsigned int, unsigned int ),
      op_add( unsigned int, unsigned int ),
      op_sub( unsigned int, unsigned int ),
      op_return( unsigned int, unsigned int ),
      op_self( unsigned int, unsigned int ),
      op_name( unsigned int, unsigned int ),
      op_pop( unsigned int, unsigned int ),
      op_pop_list( unsigned int, unsigned int ),
      op_pop_set( unsigned int, unsigned int ),
      op_pop_map( unsigned int, unsigned int ),
      op_selector( unsigned int, unsigned int ),
      op_ticks( unsigned int, unsigned int ),
      op_flatten( unsigned int, unsigned int ),
      op_caller( unsigned int, unsigned int ),
      op_source( unsigned int, unsigned int ),
      op_args( unsigned int, unsigned int ),
      op_slots( unsigned int, unsigned int ),
      op_setvar( unsigned int, unsigned int ),
      op_getvar( unsigned int, unsigned int ),    
      op_declname( unsigned int, unsigned int ),
      op_setname( unsigned int, unsigned int ),
      op_rmname( unsigned int, unsigned int ),
      op_getname( unsigned int, unsigned int ),
      op_declprivate( unsigned int, unsigned int ),
      op_setprivate( unsigned int, unsigned int ),
      op_rmprivate( unsigned int, unsigned int ),
      op_getprivate( unsigned int, unsigned int ),
      op_declverb( unsigned int, unsigned int ),
      op_setverb( unsigned int, unsigned int ),
      op_rmverb( unsigned int, unsigned int ),
      op_getverb( unsigned int, unsigned int ),
      op_decldelegate( unsigned int, unsigned int ),
      op_setdelegate( unsigned int, unsigned int ),
      op_rmdelegate( unsigned int, unsigned int ),
      op_getdelegate( unsigned int, unsigned int ),
      op_send( unsigned int, unsigned int ),
      op_send_like( unsigned int, unsigned int ),
      op_pass( unsigned int, unsigned int ),
      op_pass_to( unsigned int, unsigned int ),
      op_continue( unsigned int, unsigned int ),
      op_loop( unsigned int, unsigned int ),
      op_suspend( unsigned int, unsigned int ),
      op_not( unsigned int, unsigned int ),
      op_neg( unsigned int, unsigned int ),
      op_inc( unsigned int, unsigned int ),
      op_dec( unsigned int, unsigned int ),
      op_pos( unsigned int, unsigned int ),
      op_abs( unsigned int, unsigned int ),
      op_lshift( unsigned int, unsigned int ),
      op_rshift( unsigned int, unsigned int ),
      op_and( unsigned int, unsigned int ),
      op_or( unsigned int, unsigned int ),
      op_mul( unsigned int, unsigned int ),
      op_div( unsigned int, unsigned int ),
      op_mod( unsigned int, unsigned int ),
      op_equal( unsigned int, unsigned int ),
      op_isa( unsigned int, unsigned int ),
      op_nequal( unsigned int, unsigned int ),
      op_lesst( unsigned int, unsigned int ),
      op_lesste( unsigned int, unsigned int ),
      op_greatert( unsigned int, unsigned int ),
      op_greaterte( unsigned int, unsigned int ),
      op_map( unsigned int, unsigned int ),
      op_join( unsigned int, unsigned int ),
      op_if( unsigned int, unsigned int ),
      op_ifelse( unsigned int, unsigned int ),
      op_break( unsigned int, unsigned int ),
      op_getrange( unsigned int, unsigned int ),
      op_fail( unsigned int, unsigned int ),
      op_slice( unsigned int, unsigned int ),
      op_catch( unsigned int, unsigned int ),
      op_throw( unsigned int, unsigned int ),
      op_perform( unsigned int, unsigned int ),
      op_scatter( unsigned int, unsigned int ),
      op_band( unsigned int, unsigned int ),
      op_bor( unsigned int, unsigned int ),
      op_xor( unsigned int, unsigned int ),
      op_make_lambda( unsigned int, unsigned int ),
      op_closure( unsigned int, unsigned int ),
      op_j( unsigned int, unsigned int ),
      op_make_object( unsigned int, unsigned int ),
      op_destroy( unsigned int, unsigned int ),
      op_notify( unsigned int, unsigned int ),
      op_detach( unsigned int, unsigned int ),
      op_eval( unsigned int, unsigned int ),    
      op_trampoline( unsigned int, unsigned int );      

    void execute_opcode( const Op &opcode );
  };


}

#endif /** FRAME_HH **/


/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <cassert>
#include <vector>

#include "Data.hh"
#include "Var.hh"
#include "Exceptions.hh"
#include "Symbol.hh"
#include "String.hh"
#include "List.hh"
#include "Map.hh"
#include "Set.hh"
#include "Object.hh"
#include "Pool.hh"
#include "Pools.hh"



#include "Block.hh"

#include "Error.hh"
#include "NativeBind.hh"
#include "VariableStorage.hh"
#include "GlobalSymbols.hh"

#include "Unserializer.hh"
#include "Timer.hh"
#include "Message.hh"
#include "AbstractClosure.hh"
#include "NativeClosure.hh"
#include "Closure.hh"
#include "VariableStorage.hh"
#include "NativeBlock.hh"

using namespace mica;
using namespace std;

#ifdef BROKEN_TEMPLATE_FUNCTIONS

#define UnPack( N ) rep.copy( pos, sizeof(N), reinterpret_cast<char*>(&N) ); \
                    pos += sizeof(N);

#else

template<class T>
void Unserializer::UnPack(T &N) {
  rep.copy( pos, sizeof(N), reinterpret_cast<char *>(&N) );
  pos += sizeof(N);
}

#endif

mica_string Unserializer::readString()
{
  /** Get string size.
   */
  size_t x;
  UnPack(x);

  assert( pos + x <= rep.size() );

  /** Get string characters.
   */
  mica_string res(rep.begin()+pos, rep.begin()+pos+x);

  pos += x;

  return res;
}

Unserializer::Unserializer( const mica_string &irep)
  : rep(irep), pos(0)
{}

Var Unserializer::parse()
{
  return parseVar();
}

Var Unserializer::parseVar()
{
  /** First grab the type
   */
  Type::Identifier type;
  
  UnPack( type );

  switch(type) 
  {
  case Type::INTEGER: 
  {
      int result;
      UnPack( result );
      return Var( result );
  }
  case Type::FLOAT: 
  {
      float result;
      assert(0);
      UnPack( result );
      //      return Var( result );
  }
  case Type::CHAR: 
  {
      char result;
      UnPack( result );
      return Var( result );
  }
  case Type::OPCODE: 
  {
      mica::Op result;
      UnPack( result );
      return Var( result );
  }
  case Type::BOOL: 
  {
      bool result;
      UnPack( result );
      return Var( result );
  }
  default:    
    return parseData();
  }    

  assert(0);

}

Var Unserializer::parseData()
{
  /** Unpack the type string.
   */
  Type::Identifier type;
  UnPack( type );

  switch (type) {
  case Type::STRING:
    return parseString();
  case Type::MAP:
    return parseMap();
  case Type::SET:
    return parseSet();
  case Type::LIST:
    return parseList();
  case Type::BLOCK:
    return parseBlock();
  case Type::NATIVEBLOCK:
    return parseNativeBlock();
  case Type::OBJECT:
    return parseObject();
  case Type::SYMBOL:
    return Var(parseSymbol());
  case Type::TASK_HANDLE:
    return parseTaskHandle();
  default:
    throw unimplemented("unknown type in unserialization");
  }
}

Var Unserializer::parseString()
{
  /** Very simple.  Just read the string.
   */
  return String::from_rope(readString());
}

Symbol Unserializer::parseSymbol()
{
  /** Read a string and then make a symbol of it
   */
  return Symbol::create( readString().c_str() );
}


Var Unserializer::parseList()
{
  /** Read element count
   */
  size_t els;
  UnPack(els);

  var_vector x;

  /** Read each element
   */
  while (els--) {
    x.push_back(parseVar());
  }

  return List::from_vector
(x);
}


Var Unserializer::parseMap()
{
  /** Read element count
   */
  size_t els;
  UnPack(els);

  var_map x;

  /** Read each element
   */
  while (els--) {
    x[parseVar()] = parseVar();
  }

  return Var( Map::from_map(x) );
}

Var Unserializer::parseSet()
{
  /** Read element count
   */
  size_t els;
  UnPack(els);

  var_set x;

  /** Read each member
   */
  while (els--) {
    Var member = parseVar();
    x.insert(member);
  }


  return Set::from_set(x);
}


Environment *Unserializer::parseEnvironment()
{
  /** An environment (physical storage of an object's slots)
   */
  Environment *env = new (aligned) Environment();

  size_t num_slots;
  UnPack(num_slots);

  while (num_slots--) {
    Symbol name( parseSymbol() );
    Var accessor(parseVar());
    Var value(parseVar());

    env->addLocal( accessor, name, value );
  }

  while (1) {
    unsigned int position;
    UnPack( position );
    if (position == END_OF_ARGS_MARKER)
      break;


    Symbol selector( parseSymbol() );
    Var definer( parseVar() );
    var_vector argument_template;
    size_t a_size;
    UnPack( a_size );
    while (a_size--)
      argument_template.push_back( parseVar() );
    Var method( parseVar() );
    
    env->set_verb_parasite( selector, position, argument_template,
			    definer, method );


  }

  return env;
}

Var Unserializer::parseObject()
{
  /** Read the pool name and index.
   */
  Symbol poolName(parseSymbol());

  size_t id;
  UnPack(id);
 
  /** Get the object for it.
   */
  return Var(Pools::instance.find_pool_by_name(poolName)->resolve(id));
}




var_vector Unserializer::readVarVector()
{
  size_t sz;
  UnPack( sz );
  var_vector x;
  while (sz--)
    x.push_back( parseVar() );

  return x;
}

std::vector<int> Unserializer::readIntVector()
{
  size_t sz;
  UnPack( sz );
  std::vector<int> x;
  while (sz--) {
    int val;
    UnPack( val );
    x.push_back( val );
  }

  return x;
}

Block *Unserializer::parseBlockCommon( Block *block ) {
  block->code = readVarVector();
  block->source = readString();
  block->statements = readIntVector();
  block->line_nos = readIntVector();
  UnPack(block->add_scope);

  return block;
}

Var Unserializer::parseBlock()
{
  Var block = new (aligned) Block("tmp");

  return parseBlockCommon(block->asType<Block*>());
}


Var Unserializer::parseError()
{
  Symbol err_sym(parseSymbol());
  bool has_desc = false;
  UnPack( has_desc );
  Ref<String> desc(0);
  if (has_desc)
    desc = parseData()->asRef<String>();
  
  return new (aligned)  Error(err_sym, desc);
}


BlockContext Unserializer::parseBlockContext()
{
  BlockContext block_context;
  UnPack( block_context.scope_width );
  UnPack( block_context.opcodes );

  unsigned int numhandlers;
  UnPack( numhandlers );
  while (numhandlers--) {
    unsigned int jmp;
    unsigned int var;
    
    UnPack( jmp );
    UnPack( var );

    BlockContext::handler_entry handler( parseSymbol(),
					 jmp, var );
    block_context.handlers.push_back( handler );
  }

  return block_context;
}

VariableStorage Unserializer::parseVariableStorage()
{
  VariableStorage res;

  res.values = readVarVector();

  return res;
}

Var Unserializer::parseTaskHandle() {
  /** Read the pool name and index.
   */
  Symbol poolName(parseSymbol());

  TID tid;
  UnPack(tid);
 
  /** Get the task for it.
   */
  return Pools::instance.find_pool_by_name(poolName)->retrieve_task(tid);
}

Var Unserializer::parseNativeBlock()
{
  mica_string library = readString();
  mica_string symbol = readString();

  return loadNative( library, symbol )->asType<Data*>();
}

Ref<Task> Unserializer::parseTaskReal()
{
  Type::Identifier type_id;
  UnPack( type_id );

  Task *task;
  switch (type_id) {
  case Type::TASK:
    task = new (aligned) Task();
    break;
  case Type::CLOSURE:
    task = new (aligned) Closure();
    break;
  case Type::NATIVECLOSURE:
    task = new (aligned) NativeClosure();
    break;
  default:
    cerr << type_id << endl;
    throw internal_error("invalid task type in task unserialization");
  }

  /** Read the refcnt
   */
  int refcnt;
  UnPack( refcnt );
  task->refcnt = refcnt;

  /** Read the pool name and index.
   */
  Symbol poolName(parseSymbol());

  UnPack(task->tid);
 
  /** Get the task for it.
   */
  task->pid = Pools::instance.find_pool_by_name(poolName)->pid;

  /** Parent task
   */
  bool exists;
  UnPack( exists );
  if (exists) 
    task->parent_task = parseTaskHandle()->asRef<Task>();

  UnPack( task->msg_id );
  UnPack( task->age );
  UnPack( task->ticks ); 
  UnPack( task->time_to_live );
  UnPack( task->expire_timer ); 
  UnPack( task->terminated ); 
  UnPack( task->blocked );

  size_t children_size;
  UnPack( children_size );
  while( children_size--) {
    task->children.push_back( parseMessage() );
  }

  if (type_id == Type::CLOSURE)
    fillInClosure(task);
  else if (type_id == Type::NATIVECLOSURE)
    fillInNativeClosure(task);

  return task;
}

void Unserializer::fillInAbstractClosure( Task *task ) {
  AbstractClosure *ac = dynamic_cast<AbstractClosure*>(task);

  ac->source = parseVar();
  ac->caller = parseVar();
  ac->self = parseVar();
  ac->on = parseVar();

  ac->selector = parseSymbol();

  ac->definer = parseVar();

  size_t args_size;
  UnPack( args_size );

  while (args_size--) {
    ac->args.push_back( parseVar() );
  }

}

void Unserializer::fillInClosure( Task *task ) {
  fillInAbstractClosure( task );

  Closure *closure = dynamic_cast<Closure*>(task);

  UnPack(closure->_pc);

  size_t ls_size;
  UnPack( ls_size );
  while (ls_size--) {
    pair<unsigned int, unsigned int> loop_pair;
    UnPack( loop_pair.first );
    UnPack( loop_pair.second );
    closure->loop_stack.push_back( loop_pair );
  }

  closure->set_block( parseData()->asRef<Block>() );

  size_t stack_size;
  UnPack( stack_size );
  while (stack_size--) {
    closure->stack.push_back( parseVar() );
  }

  size_t e_stack_size;
  UnPack( e_stack_size );
  while (e_stack_size--) {
    closure->exec_stack.push_back( parseVar() );
  }

  closure->scope = parseVariableStorage();

  size_t bs_size;
  UnPack( bs_size );
  while ( bs_size-- ) {
    closure->bstck.push_back( parseBlockContext() );
  }
    
  UnPack( closure->ex_state );
}

void Unserializer::fillInNativeClosure( Task *task ) {
  fillInAbstractClosure( task );

  dynamic_cast<NativeClosure*>(task)->native_block = parseData()->asRef<NativeBlock>();
}

Ref<Message> Unserializer::parseMessage() {
  Type::Identifier type_id;
  UnPack( type_id );

  Message *msg;
  switch( type_id ) {
  case Type::MESSAGE:
    msg = new (aligned) Message();
    break;
  case Type::RETURNMESSAGE:
    msg = new (aligned) ReturnMessage();
    break;
  case Type::RAISEMESSAGE:
    msg = new (aligned) RaiseMessage();
    break;
  case Type::HALTMESSAGE:
    msg = new (aligned) HaltMessage();
    break;
  default:
    throw internal_error("invalid type_id in Unserializer::parseMessage");
  }
  
  bool exists;
  UnPack( exists );
  if (exists)
    msg->parent_task = parseTaskHandle()->asRef<Task>();

  UnPack( msg->msg_id );
  UnPack( msg->age );
  UnPack( msg->ticks );

  msg->source = parseVar();
  msg->caller = parseVar();
  msg->self = parseVar();
  msg->on = parseVar();

  msg->selector = parseSymbol();

  size_t args_size;
  UnPack( args_size );

  while (args_size--) {
    msg->args.push_back( parseVar() );
  }
  
  return msg;
}

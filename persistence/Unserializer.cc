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
#include "GlobalSymbols.hh"

#include "Unserializer.hh"
#include "Timer.hh"
#include "Message.hh"
#include "AbstractFrame.hh"
#include "NativeFrame.hh"
#include "Frame.hh"

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


OStorage *Unserializer::parseOStorage()
{
  /** An environment (physical storage of an object's slots)
   */
  OStorage *env = new (aligned) OStorage();

  while (1) {
    bool more;
    UnPack( more );
    if ( more ) {
      Symbol name( parseSymbol() );
      cerr << name.tostring() << endl;
      Var accessor(parseVar());
      Var value(parseVar());
      


      env->addLocal( accessor, name, value );
    } else {
      break;
    }
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
  case Type::FRAME:
    task = new (aligned) Frame();
    break;
  case Type::NATIVEFRAME:
    task = new (aligned) NativeFrame();
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

  if (type_id == Type::FRAME)
    fillInFrame(task);
  else if (type_id == Type::NATIVEFRAME)
    fillInNativeFrame(task);

  return task;
}

void Unserializer::fillInAbstractFrame( Task *task ) {
  AbstractFrame *ac = dynamic_cast<AbstractFrame*>(task);

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

void Unserializer::fillInFrame( Task *task ) {
  fillInAbstractFrame( task );

  Frame *frame = dynamic_cast<Frame*>(task);

  UnPack(frame->control._pc);


  frame->control.set_block( parseData()->asRef<Block>() );

  size_t stack_size;
  UnPack( stack_size );
  while (stack_size--) {
    frame->stack.push_back( parseVar() );
  }

   
  UnPack( frame->ex_state );
}

void Unserializer::fillInNativeFrame( Task *task ) {
  fillInAbstractFrame( task );

  dynamic_cast<NativeFrame*>(task)->native_block = parseData()->asRef<NativeBlock>();
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

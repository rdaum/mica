/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef UNSERIALIZER_HH
#define UNSERIALIZER_HH

#include "Task.hh"
#include "VariableStorage.hh"
#include "BlockContext.hh"

namespace mica {

  class Block;
  class Environment;

  class Closure;
  class NativeClosure;
  class AbstractClosure;
  class Message;

  class Unserializer
  {
  public:
    Unserializer( const rope_string &rep );
    Environment* parseEnvironment();

    Var parse();
    Var parseData();
    var_vector readVarVector();

  public:
    Ref<Task> parseTaskReal() ;

  private:
    void fillInAbstractClosure( Task *task );
    void fillInClosure( Task *task );
    void fillInNativeClosure( Task *task );

    Ref<Message> parseMessage();

  private:
    rope_string rep;
    size_t pos;

  private:
    template<class T>
    void UnPack( T &N );

    rope_string readString();

    Var parseVar();
    
    Var parseString();
    Symbol parseSymbol();
    Var parseError();
    Var parseList();
    Var parseMap();
    Var parseSet();
    Var parseObject();
    Var parseProxyData();
    Var parseBlock();
    Var parseTaskHandle();
    Block* parseBlockCommon( Block *block );

    Var parseNativeBlock();

    BlockContext parseBlockContext();
    VariableStorage parseVariableStorage();



    std::vector<int> readIntVector();
  };

}


#endif

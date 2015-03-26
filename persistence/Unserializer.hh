/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef PERSISTENCE_UNSERIALIZER_HH
#define PERSISTENCE_UNSERIALIZER_HH

#include "vm/Task.hh"

namespace mica {

class Block;
class OStorage;

class Frame;
class Message;

class Unserializer {
 public:
  Unserializer(const mica_string &rep);
  OStorage *parseOStorage();

  Var parse();
  Var parseData();
  var_vector readVarVector();

 public:
  Ref<Task> parseTaskReal();

 private:
  void fillInFrame(Task *task);

  Ref<Message> parseMessage();

 private:
  mica_string rep;
  size_t pos;

 private:
  template <class T>
  void UnPack(T &N);

  mica_string readString();

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
  Block *parseBlockCommon(Block *block);

  std::vector<int> readIntVector();
};
}  // namespace mica

#endif  // PERSISTENCE_UNSERIALIZER_HH


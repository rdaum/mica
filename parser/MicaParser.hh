#ifndef MICA_PARSER_HH
#define MICA_PARSER_HH

#include <sstream>
#include <string>

#include "parser/Nodes.hh"
#include "parser/parser.h"
#include "types/Var.hh"

namespace mica {

class micaParser {
 public:
  micaParser(const std::string& program);

  std::string program;

  NPtr parse();

 public:
  NPtr translateProgram(Whale::NonterminalProgram* program);
  NPtr translateStatement(Whale::NonterminalStatement* statement);
  NPtr translateExpr(Whale::NonterminalExpr* expr);

  template <typename T>
  NPtr translateBinaryLhs(T bexpr) {
    return translateExpr(bexpr->leftArg);
  }

  NPtr translateBinaryExpr(const NPtr& left, Whale::NonterminalBinaryExpr* bexpr);

  NPtr translateDecl(Whale::NonterminalDecl* decl);
  NPtr translateRemove(Whale::NonterminalRemove* rm);
  NPtr translateVar(Whale::NonterminalVar* var);
  NPtr translateVarDecl(Whale::NonterminalVarDecl* varDecl);

  pair<NPtr, NPtr> translateVerbTemplt(Whale::NonterminalVerbTemplt* vs);

  pair<NPtr, NPtr> translateMsgArgs(Whale::NonterminalMsgArgs* vs);

  NPtr translateVerbArg(Whale::NonterminalVerbArg* verbarg);

  NPtr translateSlot(Whale::NonterminalSlot* slot);
  NPtr translateSlotRm(Whale::NonterminalSlot* slot);
  NPtr translateSlotDecl(Whale::NonterminalSlotDecl* slot);

  NPtr translateAssign(Whale::NonterminalVar* var, Whale::NonterminalExpr* expr);
  NPtr translateAssignSlot(Whale::NonterminalSlot* slot, Whale::NonterminalExpr* expr);
  NPtr translateAssignVar(Whale::TerminalId* var, Whale::NonterminalExpr* expr);

  NPtr translateError(Whale::NonterminalErrorValue* errorValue);

  NPtr translateListItem(Whale::NonterminalListItem* listItem);

  NPtr translateMessage(Whale::NonterminalMessage* msg, const NPtr& dest);

  NPtr translateArgDeclList(Whale::NonterminalArgDeclList* args, const NPtr& source, bool declare);

  pair<NPtr, std::string> translateFrame(pair<int, int> start_pos, Whale::NonterminalFrame* frame);

  Var translateId(Whale::TerminalId* id);

 private:
  std::string get_range(const pair<int, int>& start, const pair<int, int>& end);

  template <class T>
  NPtr get_slot_name(const T slot);

  template <class ReturnType, class ElementType, class InputIterator>
  vector<ReturnType> map_to_vec(InputIterator begin, InputIterator end,
                                mem_fun1_t<ReturnType, micaParser, ElementType> function);

  std::string translateEscapeCodes(const char* str);
  int getHexChar(const char* str);
  int getHexDigit(const char c);
};

}  // namespace mica

#endif  // MICA_PARSER_HH

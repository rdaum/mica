#include "config.h"
#include "common/mica.h"

#include <algorithm>
#include <functional>

#include <iostream>
#include <cstdlib>
#include <ctype.h>


#include <boost/lexical_cast.hpp>

#include "Var.hh"
#include "hash.hh"
#include "Symbol.hh"
#include "MicaParser.hh"
#include "parser.h"
#include "List.hh"
#include "OpCode.hh"
#include "MetaObjects.hh"
#include "String.hh"
#include "Block.hh"
#include "Error.hh"
#include "Exceptions.hh"


#if 1
#define DEBUG(x) x
#else
#define DEBUG(x)
#endif


using namespace Whale;
using namespace mica;
using namespace std;

#if __GNUC__ == 3
#define member_pointer std::mem_fun
#else
#define member_pointer std::mem_fun1
#endif

/** Here's a nice generic function to map result of a function for each
 *  element from begin -> end and return the result in a vector of
 *  values.
 */

template< class ReturnType, class ElementType, class InputIterator >
vector<ReturnType> 
micaParser::map_to_vec( InputIterator begin, InputIterator end,
		       mem_fun1_t<ReturnType, micaParser, ElementType> function )
{
  vector<ReturnType> results_vector;
  InputIterator statement_it;
  for (statement_it = begin; statement_it != end; statement_it++) {
    ElementType statement = *statement_it;
    results_vector.push_back( function( this, statement ) );
  }
  return results_vector; 
}


micaParser::micaParser( const std::string &in_program )
  : program(in_program)
{}

NPtr micaParser::parse()
{
  istringstream streamIn(program);
  DolphinLexicalAnalyzer lexer(streamIn);
  WhaleParser parser(lexer);
  Whale::NonterminalProgram* parseTree = parser.parse();

  return translateProgram(parseTree);
}

NPtr micaParser::translateProgram(NonterminalProgram* program)
{
  vector<NPtr> stmts( map_to_vec( program->statementList.begin(),
				  program->statementList.end(),
				  member_pointer(&micaParser::translateStatement) ) );
  
  return new (aligned) blockNode( stmts );
}

NPtr micaParser::translateStatement(NonterminalStatement* statement)
{
  if (statement->sideEffectExpr) {
    return translateExpr(statement->sideEffectExpr);
  } else if (statement->ifStatement) {
    NPtr testExpr(translateExpr(statement->testExpr));
    NPtr trueBranch(translateStatement(statement->ifStatement));

    if (statement->elseStatement) {
      NPtr elseBranch( translateStatement(statement->elseStatement) );
      return new (aligned) ifElseNode( testExpr, trueBranch, elseBranch );
    } else {
      return new (aligned) ifNode( testExpr, trueBranch );
    }
  } else if (statement->isWhileLoop) {
    NPtr testExpr( translateExpr(statement->testExpr) );
    NPtr trueBranch( translateStatement(statement->body) );
    return new (aligned) whileNode( testExpr, trueBranch );
  } else if (statement->isDoWhileLoop) {
    NPtr testExpr( translateExpr(statement->testExpr) );
    NPtr trueBranch( translateStatement(statement->body) );
    return new (aligned) doWhileNode( testExpr, trueBranch );
  } else if (statement->iterator) {
    NPtr forVar( translateVar(statement->iterator) );
    NPtr rangeExpr( translateExpr(statement->container) );
    NPtr doStmt(  translateStatement(statement->body) );

    return new (aligned) forNode( forVar, rangeExpr, doStmt );
  } else if (statement->tryBody) {
    NPtr body( translateStatement( statement->tryBody) );
    
    vector< tryCatchNode::Catch > catchers;
    for ( vector<NonterminalCatchStatement*>::iterator x = statement->catchList.begin(); x != statement->catchList.end(); x++ ) {
      NonterminalCatchStatement *c = *x;
      tryCatchNode::Catch catcher;
      catcher.ident = translateId(c->var);
      catcher.err = new (aligned) Error( Symbol::create(c->error->text + 1), Ref<String>(0) );
      catcher.branch = translateStatement( c->body );

      catchers.push_back( catcher );
    }
    return new (aligned) tryCatchNode( body, catchers );
  } else if (statement->throwError) {
    NPtr err( translateError(statement->throwError) );
    return new (aligned) throwNode( err );
  } else if (statement->scatterSource) {
    bool declare = (statement->declaringVars ? true : false);
    return translateArgDeclList( statement->args,
				 translateExpr(statement->scatterSource),
				 declare );

  } else if (!statement->declList.empty()) {
    vector<NPtr> declares( map_to_vec( statement->declList.begin(),
				       statement->declList.end(),
				       member_pointer(&micaParser::translateDecl) ) );
    return new (aligned) stmtListNode( declares );
  } else if (statement->returnValue) {
    return new (aligned) returnNode( translateExpr(statement->returnValue ) );
  } else if (statement->isContinue) {
    return new (aligned) literalNode( Var(Op::CONTINUE) );
  } else if (statement->isBreak) {
    return new (aligned) literalNode( Var(Op::BREAK) );
  } else if (statement->isAssign) {
    return translateAssign( statement->isAssign,
			    statement->value );
  } else if (!statement->removeList.empty()) {
    return new (aligned) stmtListNode( map_to_vec( statement->removeList.begin(),
					 statement->removeList.end(),
					 member_pointer(&micaParser::translateRemove ) ));
  } else if (statement->notifyExpr) {
    return new (aligned) unaryNode( translateExpr(statement->notifyExpr), Var(Op::NOTIFY) ) ;

  } else if (statement->isDetach) {
    return new (aligned) literalNode( Var(Op::DETACH) );

  } else {
    return new (aligned) blockNode( vector<NPtr>( map_to_vec( statement->statementList.begin(), statement->statementList.end(),  member_pointer(&micaParser::translateStatement) ) ));
  }
}


std::string::iterator find_position( std::string &str,
				    int line, int column ) {
  
  std::string::iterator pos(str.begin());

  int counted_lines = 0;
  while (line != counted_lines) {
    if (pos == str.end())
      return pos;

    if ( (*pos) == '\n')
      counted_lines++;

    pos++;
  }
  int counted_column = 0;
  while (counted_column != column) {
    if (pos == str.end())
      return pos;

    counted_column++;
    pos++;
  }
  return pos;
}

std::string micaParser::get_range( const pair<int, int> &start,
				  const pair<int, int> &end ) {

  return std::string( find_position( program, start.first, start.second ),
		      find_position( program, end.first, end.second ) );
}



NPtr micaParser::translateArgDeclList( NonterminalArgDeclList *args,
				      const NPtr &source,
				      bool declare ) {
  var_vector mandatory(map_to_vec(args->mandatoryVarList.begin(),
				  args->mandatoryVarList.end(),
				  member_pointer(&micaParser::translateId)));
    
  var_vector optional(map_to_vec(args->optionalVarList.begin(),
				 args->optionalVarList.end(),
				 member_pointer(&micaParser::translateId) ));

  Var remainder(false);
  if (args->remainderVar)
    remainder = translateId(args->remainderVar);

  return new (aligned) scatterAssignNode( source,
					  mandatory, optional, remainder,
					  declare );
}

struct operator_info {
  const char *text;
  Op::Code opcode;
};

static operator_info unary_operations[] = {
  "-", Op::NEG,
  "!", Op::NOT,
};

static operator_info builtins[] = {
  "self", Op::SELF,
  "slots", Op::SLOTS,
  "source", Op::SOURCE,
  "caller", Op::CALLER,
  "selector", Op::SELECTOR,
  "args", Op::ARGS
};

static operator_info binary_operations[] = {
  "+", Op::ADD,
  "-", Op::SUB,
  "/", Op::DIV,
  "*", Op::MUL,
  "%", Op::MOD,
  "=", Op::EQUAL,
  "!=", Op::NEQUAL,
  "isA", Op::ISA,
  "&&", Op::AND,
  "||", Op::OR,
  "|", Op::BOR,
  "&", Op::BAND,
  "^", Op::XOR,
  "<<", Op::LSHIFT,
  ">>", Op::RSHIFT,
  "<", Op::LESST,
  ">", Op::GREATERT
};

NPtr micaParser::translateExpr(NonterminalExpr* expr)
{
  if (expr->parenthesizedExpr) {
    return translateExpr(expr->parenthesizedExpr);
  } else if (expr->binaryExpr) {
    return translateBinaryExpr( translateBinaryLhs( expr->binaryExpr ), expr->binaryExpr );
  } else if (expr->unaryArg) {
    Var opcode;
    for (int i = 0; i < sizeof(unary_operations)/sizeof(operator_info);
	 i++) {
      if (!strcmp(expr->op->text, unary_operations[i].text)) {
	opcode = Op(unary_operations[i].opcode);
	break;
      }
    }
    if (opcode == NONE) {
      cerr << "GEH" << endl;
      throw internal_error("unable to find opcode to match unary expression operator");
    }

    return new (aligned) unaryNode( translateExpr(expr->unaryArg), opcode );

  } else if (expr->builtin) {
    Var opcode;
    for (int i = 0; i < sizeof(builtins)/sizeof(operator_info);
	 i++) {
      if (!strcmp(expr->builtin->text, builtins[i].text)) {
	opcode = Op(builtins[i].opcode);
	break;
      }
    }
    if (opcode == NONE)
      throw internal_error("unable to find opcode to match builtin expression");
    
    return new (aligned) literalNode(opcode);

  } else if (expr->isList) {
    return new (aligned) listNode( map_to_vec( expr->listItemList.begin(),
				     expr->listItemList.end(),
				     member_pointer(&micaParser::translateListItem) ));
  } else if (expr->isSet) {
    return new (aligned) setNode( map_to_vec( expr->setItemList.begin(),
				    expr->setItemList.end(),
				    member_pointer(&micaParser::translateListItem) ));
  } else if (expr->isMap) {
    vector<NPtr> map_vec;
    vector<NonterminalMapEntry*>::iterator map_it;
    for (map_it = expr->mapEntryList.begin();
	 map_it != expr->mapEntryList.end(); map_it++) {
      NonterminalMapEntry *mapEntry = *map_it;
      map_vec.push_back( translateExpr(mapEntry->key) );
      map_vec.push_back( translateExpr(mapEntry->value) );
    }
    return new (aligned) mapNode(map_vec);
  } else if (expr->containerExpr) {

    if (expr->index && ! expr->end)
      return new (aligned) binaryNode( translateExpr(expr->containerExpr),
			     translateExpr(expr->index),
			     Var(Op::SLICE) );
    else 
      return new (aligned) trinaryNode( translateExpr(expr->containerExpr),
			      translateExpr(expr->index),
			      translateExpr(expr->end),
			      Var(Op::GETRANGE) );

  } else if (expr->integerLiteral) {
    int number_value = boost::lexical_cast<int>(expr->integerLiteral->text);
    return new (aligned) literalNode( Var(number_value) );
  } else if (expr->floatLiteral) {
    float number_value = 
      boost::lexical_cast<float>(expr->floatLiteral->text);
    assert(0);
    //    return new (aligned) literalNode( Var(number_value) );
  } else if (expr->charLiteral) {
    return new (aligned) literalNode( Var(expr->charLiteral->text[1]) );
  } else if (!expr->stringLiteralList.empty()) {
    // Concatenate a sequence of directly adjacent string literals
    std::vector<TerminalStringLiteral*>::iterator iter;
    string str;
    for(iter=expr->stringLiteralList.begin();
	iter!=expr->stringLiteralList.end();
	iter++)
      {
	str.append( translateEscapeCodes((*iter)->text));
      }
  
    return new (aligned) literalNode( String::from_cstr(str.c_str()) );

  } else if (expr->symbol) {
    return new (aligned) literalNode( Var(Symbol::create( expr->symbol->text + 1)) );
  } else if (expr->var) {
    return translateVar(expr->var);
  } else if (expr->error) {
    return translateError(expr->error);
  } else if (expr->isMethod) {
    pair<int,int> 
      start_pos( make_pair( expr->isMethod->line - 1, 
			    expr->isMethod->column -1 ) );
  

    pair<NPtr, std::string> 
      result( translateClosure( start_pos,
				expr->closure ) );
    return new (aligned) methodNode( result.first, result.second.c_str() );
  } else if (expr->isLambda) {
    pair<int,int> 
      start_pos( make_pair( expr->isLambda->line - 1, 
			    expr->isLambda->column -1 ) );
  

    pair<NPtr, std::string> 
      result( translateClosure( start_pos,
				expr->closure ) );
    return new (aligned) lambdaNode( result.first, result.second.c_str() );
  } else if (expr->isObjectConstruct) {
    NonterminalObjectConstruct *block = expr->isObjectConstruct;

    NPtr 
      o_block(new (aligned) blockNode( map_to_vec( block->statementList.begin(),
						   block->statementList.end(), 
						   member_pointer(&micaParser::
							translateStatement))
				       )
	      );
    
    return new (aligned) objectConstructorNode( o_block );

  } else if (expr->boolT) {
    return new (aligned) literalNode( Var(true) );
  } else if (expr->boolF) {
    return new (aligned) literalNode( Var(false) );
  }

}
 

NPtr micaParser::translateBinaryExpr( const NPtr &left, NonterminalBinaryExpr* expr )
{
  if (expr->rightArg) {
    Var opcode;
    for (int i = 0; i < sizeof(binary_operations)/sizeof(operator_info);
	 i++) {
      if (!strcmp(expr->op->text, binary_operations[i].text)) {
	opcode = Op(binary_operations[i].opcode);
	break;
      }
    }
    if (opcode == NONE)
      throw internal_error("unable to find opcode to match binary expression operator");
    
    return new (aligned) binaryNode( translateExpr(expr->rightArg),
				     left,
				     opcode );
  } else if (!expr->argList.empty()) {
    NPtr arguments = new (aligned) listNode( map_to_vec( expr->argList.begin(),
							 expr->argList.end(),
							 member_pointer(&micaParser::translateListItem) ));
    
    return new (aligned) binaryNode( left, arguments, Var(Op::PERFORM) );
  } else if (expr->messageExpr) {
    return translateMessage( expr->messageExpr,
			     left );
  } 

}

pair<NPtr, std::string> 
micaParser::translateClosure( pair<int,int> start_pos,
			     NonterminalClosure *closure ) {
  vector<NPtr> statements;
  
  if (closure->argslist && closure->argslist->args) {
    NPtr args_declare = translateArgDeclList( closure->argslist->args,
					      new (aligned) literalNode(Var(Op::ARGS)),
					      true );

    statements.push_back( args_declare );
  }
    
  NPtr stmts = new (aligned)
    stmtListNode( map_to_vec( closure->statementList.begin(),
			      closure->statementList.end(), 
			      member_pointer(&micaParser::translateStatement)));

  statements.push_back( stmts );

  pair<int,int> end_pos( make_pair( closure->endpos->line - 1,
				    closure->endpos->column ) );
    

  std::string program( get_range( start_pos, end_pos ) );
    
  return make_pair( new (aligned) blockNode( statements ), program );
}


NPtr micaParser::translateVar(NonterminalVar* var)
{
  if (var->slot)
    return translateSlot(var->slot);
  else if (var->id)
    return new (aligned) identNode(translateId(var->id));
}

NPtr micaParser::translateError(NonterminalErrorValue *errorValue)
{
  Symbol errorId = Symbol::create(errorValue->errorSymbol->text + 1);
  Ref<String> errorStr(0);
  if (errorValue->errorArgumentString)
    errorStr = String::
      from_cstr( translateEscapeCodes(errorValue->
				      errorArgumentString->text).c_str())->asRef<String>();
  return new (aligned) errorNode( errorId, errorStr );
}


Var micaParser::translateId(TerminalId* id)
{
  return Var(Symbol::create(id->text));
}

template< class T >
NPtr micaParser::get_slot_name( const T slot ) {
  if (slot->slotNameExpr)
    return translateExpr( slot->slotNameExpr );
  else
    return new (aligned) literalNode( Var(Symbol::create(slot->slotName->text )) );
}


NPtr micaParser::translateMessage( NonterminalMessage* msg, 
				  const NPtr &destination ) {
 
  pair<NPtr, NPtr> result = translateMsgArgs( msg->verbT );
  NPtr selector = result.first;
  NPtr args = result.second;
  
  if (msg->qualifier) 
    return new (aligned) qualifiedMessageNode( destination, selector, args, 
				     translateExpr( msg->qualifier->asObj ) );
  else
    return new (aligned) messageNode( destination, selector, args );
}

NPtr micaParser::translateRemove( NonterminalRemove *remove ) {

  /** either a slot or a variable.
   */
  if (remove->var->slot)
    return translateSlotRm( remove->var->slot );
  else
    throw unimplemented("variable removable unimplemented");
}

NPtr micaParser::translateSlotRm( NonterminalSlot* slot ) {
  Var opcode;
  NPtr slot_name;

  if (slot->verbSlot) {

    pair<NPtr, NPtr> vs_info = translateVerbTemplt(slot->verbSlot->verbT);
    
    return new (aligned) rmVerbNode( vs_info.first, vs_info.second );

  } else if (slot->privateSlot) {
    opcode = Op::RMPRIVATE;
    slot_name = get_slot_name( slot->privateSlot );
  } else if (slot->delegateSlot) {
    opcode = Op::RMDELEGATE;
    slot_name = get_slot_name( slot->delegateSlot );
  } else if (slot->nameSlot) {
    opcode = Op::RMNAME;
    slot_name = get_slot_name( slot->nameSlot );
  }

  return new (aligned) unaryNode( slot_name, opcode );  
}


NPtr micaParser::translateDecl(NonterminalDecl* decl )
{
  /** It's either a slot or var declaration.  Choose now.
   */
  if (decl->varDecl)
    return translateVarDecl( decl->varDecl );
  else
    return translateSlotDecl( decl->slotDecl );
}

NPtr micaParser::translateVarDecl(NonterminalVarDecl* varDecl)
{
  Var id( translateId(varDecl->id) );
  NPtr initial;
  if (varDecl->initialValue) {
    initial = translateExpr(varDecl->initialValue);
  } else {
    initial = new (aligned) literalNode(NONE);
  }
  return new (aligned) varDeclNode( id, initial );
}

NPtr micaParser::translateVerbArg(NonterminalVerbArg* varg)
{
  if (varg->argexpr)
    return translateExpr(varg->argexpr);
  else if (varg->wildcard)
    return new (aligned) literalNode( MetaObjects::AnyMeta );

  assert(0);
}

pair<NPtr, NPtr> micaParser::translateVerbTemplt( NonterminalVerbTemplt *vs ) {

  mica_string selector_name;

  if (vs->rootSel->text)
    selector_name = vs->rootSel->text;
  
  for (vector<TerminalId*>::iterator x = vs->moreSels.begin();
       x != vs->moreSels.end();x++) {
    selector_name.push_back('_');
    selector_name.append( (*x)->text );
  }
  NPtr selector = new (aligned) literalNode( Var(Symbol::create( selector_name.c_str() )) );
  
  vector<NPtr> args_vec;
  if (vs->rootArg)
    args_vec.push_back( translateVerbArg( vs->rootArg ) );

  vector<NPtr> 
    args_vec2(map_to_vec(vs->args.begin(),
			 vs->args.end(),
			 member_pointer(&micaParser::translateVerbArg)));
  
  args_vec.insert( args_vec.end(), args_vec2.begin(), args_vec2.end() );

  NPtr args = new (aligned) listNode( args_vec );   

  return make_pair( selector, args );
}


pair<NPtr, NPtr> micaParser::translateMsgArgs( NonterminalMsgArgs *vs ) {

  mica_string selector_name;

  if (vs->rootSel->text)
    selector_name = vs->rootSel->text;
  
  for (vector<TerminalId*>::iterator x = vs->moreSels.begin();
       x != vs->moreSels.end();x++) {
    selector_name.push_back('_');
    selector_name.append( (*x)->text );
  }
  NPtr selector = new (aligned) literalNode( Var(Symbol::create( selector_name.c_str() )) );
  
  vector<NPtr> args_vec;
  if (vs->rootArg)
    args_vec.push_back( translateExpr( vs->rootArg ) );

  vector<NPtr> 
    args_vec2(map_to_vec(vs->args.begin(),
			 vs->args.end(),
			 member_pointer(&micaParser::translateExpr)));
  
  args_vec.insert( args_vec.end(), args_vec2.begin(), args_vec2.end() );

  NPtr args = new (aligned) listNode( args_vec );   

  return make_pair( selector, args );
}

NPtr micaParser::translateSlotDecl(NonterminalSlotDecl* slotDecl )
{
  /** A slot is one of 4 types:
   *  verbSlot, privateSlot, delegateSlot, nameSlot
   *  Each has a structure: slotName | slotNameExpr
   */
  Op::Code opcode;
  NPtr name;

  NPtr value;
  if (slotDecl->initialValue) {
    value = translateExpr( slotDecl->initialValue );
  } else {
    value = new (aligned) literalNode( NONE );
  }

  if (slotDecl->slot->verbSlot) {
    pair<NPtr, NPtr> vs_info( translateVerbTemplt(slotDecl->slot->verbSlot->verbT) );
    
    return new (aligned) declVerbNode( vs_info.first, vs_info.second, value );
    
  } else if (slotDecl->slot->privateSlot) {
    opcode = Op::DECLPRIVATE;
    name = get_slot_name( slotDecl->slot->privateSlot );
  } else if (slotDecl->slot->delegateSlot) {
    opcode = Op::DECLDELEGATE;
    name = get_slot_name( slotDecl->slot->delegateSlot );
  } else if (slotDecl->slot->nameSlot) {
    opcode = Op::DECLNAME;
    name = get_slot_name( slotDecl->slot->nameSlot );
  }

  return new (aligned) binaryNode( value, name, Var( Op( opcode ) ) );
}

NPtr micaParser::translateSlot(NonterminalSlot* slot)
{
  /** A slot is one of 4 types:
   *  verbSlot, privateSlot, delegateSlot, nameSlot
   *  Each has a structure: slotName | slotNameExpr
   */
  Op::Code opcode;
  NPtr name;

  if (slot->verbSlot) {

    pair<NPtr, NPtr> vs_info( translateVerbTemplt(slot->verbSlot->verbT) );
    
    return new (aligned) getVerbNode( vs_info.first, vs_info.second );

  } else if (slot->privateSlot) {
    opcode = Op::GETPRIVATE;
    name = get_slot_name( slot->privateSlot );
  } else if (slot->delegateSlot) {
    opcode = Op::GETDELEGATE;
    name = get_slot_name( slot->delegateSlot );
  } else if (slot->nameSlot) {
    opcode = Op::GETNAME;
    name = get_slot_name( slot->nameSlot );
  }

  return new (aligned) unaryNode( name, Var( Op( opcode )  ) );
}

NPtr micaParser::translateAssign(NonterminalVar* var, NonterminalExpr *expr)
{
  if (var->slot)
    return translateAssignSlot(var->slot, expr);
  else if (var->id)
    return translateAssignVar(var->id, expr);
}

NPtr micaParser::translateAssignSlot(NonterminalSlot* slot,
				    NonterminalExpr* expr) {
  /** A slot is one of 4 types:
   *  verbSlot, privateSlot, delegateSlot, nameSlot
   *  Each has a structure: slotName | slotNameExpr
   */
  Op::Code opcode;
  NPtr name;
  NPtr value = translateExpr( expr );

  if (slot->verbSlot) {

    pair<NPtr, NPtr> vs_info( translateVerbTemplt(slot->verbSlot->verbT) );
    
    return new (aligned) setVerbNode( vs_info.first, vs_info.second, value );

  } else if (slot->privateSlot) {
    opcode = Op::SETPRIVATE;
    name = get_slot_name( slot->privateSlot );
  } else if (slot->delegateSlot) {
    opcode = Op::SETDELEGATE;
    name = get_slot_name( slot->delegateSlot );
  } else if (slot->nameSlot) {
    opcode = Op::SETNAME;
    name = get_slot_name( slot->nameSlot );
  }

  return new (aligned) binaryNode( value, name, Var( Op( opcode ) ) );
}

NPtr micaParser::translateAssignVar(TerminalId* var,
				   NonterminalExpr *expr) {

  Var id(translateId( var ));
  NPtr value(translateExpr( expr ));

  return new (aligned) assignNode( id, value );
}


NPtr micaParser::translateListItem(NonterminalListItem* listItem)
{
  if (listItem->unaryArg) {
    return new (aligned) unaryNode( translateExpr(listItem->unaryArg), Var(Op::FLATTEN) );
  } else if (listItem->comp) {
    

    /** Mapping expression is (expr, (expr, op), map)
     */
    
    // Scan for the binary operation used
    Var opcode;
    for (int i = 0; i < sizeof(binary_operations)/sizeof(operator_info);
	 i++) {
      if (!strcmp(listItem->comp->op->text, binary_operations[i].text)) {
	opcode = Op(binary_operations[i].opcode);
	break;
      }
    }
    NPtr left = translateExpr( listItem->comp->leftArg );
    NPtr right = new (aligned) unaryNode( translateExpr(listItem->comp->rightArg),
				opcode );

    vector<NPtr> exprs;
    exprs.push_back( right );
    NPtr right_expr = new (aligned) quoteNode( exprs );

    return new (aligned) binaryNode( left, right_expr, Var(Op::MAP) );

  } else {
    return translateExpr(listItem->expr);
  }
}


std::string micaParser::translateEscapeCodes(const char* str)
{
  std::string result;
  // Skip surrounding quotes, and go through looking for
  // escape sequences. The lexer assures us all this will
  // not break.
  for(unsigned int i=1; str[i+1]; i++)
    {
      if(str[i]=='\\')
	{
	  if (isdigit(str[i+1]))
	    {
	      unsigned int charCode = 0;
	      unsigned int j;
	      for (j=i+1; j<i+1+3; j++)
		{
		  if (!isdigit(str[j])) break;
		  charCode = charCode*8 + str[j]-'0';
		}
	      i = j-1;
	      result += (char)charCode;
	    }
	  else
	    switch(str[i+1])
	      {
	      case 'a':  result += '\a'; i++; break;
	      case 'b':  result += '\b'; i++; break;
	      case 'f':  result += '\f'; i++; break;
	      case 'n':  result += '\n'; i++; break;
	      case 'r':  result += '\r'; i++; break;
	      case 't':  result += '\t'; i++; break;
	      case 'v':  result += '\v'; i++; break;
	      case '\'': result += '\''; i++; break;
	      case '\"': result += '\"'; i++; break;
	      case '\?': result += '\?'; i++; break;
	      case '\\': result += '\\'; i++; break;
	      case '\n': result += '\n'; i++; break;
	      case 'x' : result += getHexChar(str + i+2); i+=3; break;
	      }
	}
      else
	{
	  result += str[i];
	}
    }
  return result;
}

// Takes the first two chars of the string,
// treating them as hexadecimal digits, and
// returns the character with that value.
int micaParser::getHexChar(const char* str)
{
  return (char)(getHexDigit(str[0])*16 + getHexDigit(str[1]));
}

int micaParser::getHexDigit(const char c)
{
  if (isdigit(c))
    return c-'0';
  else if ('a' <= c && c <= 'f')
    return c-'a'+10;
  else if ('A' <= c && c <= 'F')
    return c-'A'+10;
  else
    {
      // TODO: error!
    }
}

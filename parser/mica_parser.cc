// #define BOOST_SPIRIT_DEBUG

#include <boost/spirit/core.hpp>
#include <boost/spirit/utility.hpp>
#include <boost/spirit/symbols.hpp>
#include <boost/spirit/tree/parse_tree.hpp>


#include <iostream>
#include <string>

#include "Var.hh"
#include "hash.hh"
#include "Symbol.hh"
#include "parser.h"
#include "List.hh"
#include "OpCode.hh"
#include "MetaObjects.hh"
#include "String.hh"
#include "Block.hh"
#include "Error.hh"
#include "Exceptions.hh"
#include "Nodes.hh"

using namespace std;
using namespace boost::spirit;

struct mica_grammar : public grammar<mica_grammar>
{
  template <typename ScannerT>
  struct definition
  {
    rule<ScannerT> list_expression, primary_expression, 
                   map_expression, map_item, statement, if_expression,
                   compound_statement, lambda_expression, assignment,
                   declare, message, while_loop, do_loop, for_range, 
                   try_catch, catch_block, loop_statement, control_statement,
                   slot_or_var, argument_mask, argument_declaration,
                   method, object, literals;

    rule<ScannerT> mandatory, optional, remainder;

    rule<ScannerT>
           postfix_expression, postfix_expression_helper,
           multiplicative_expression, multiplicative_expression_helper,
           additive_expression, additive_expression_helper,
           shift_expression, shift_expression_helper,
           relational_expression, relational_expression_helper,
           equality_expression, equality_expression_helper,
           and_expression, and_expression_helper,
           exclusive_or_expression, exclusive_or_expression_helper,
           inclusive_or_expression, inclusive_or_expression_helper,
           logical_and_expression, logical_and_expression_helper,
           logical_or_expression, logical_or_expression_helper,
           assignment_expression, argument_expression_list, 
           assignment_operator, expression, expression_helper;

    symbols<> keywords;
    
    strlit<> RIGHT_OP, LEFT_OP, INC_OP, DEC_OP, PTR_OP, AND_OP,
             OR_OP, LE_OP, GE_OP, ASSIGN, NE_OP, MAP_START, LEFT_ASSOC,
             RIGHT_ASSOC;

    chlit<>  SEMICOLON, COMMA, COLON, EQ_OP, LEFT_PAREN, RIGHT_PAREN,
             DOT, ADDROF, BANG, TILDE, MINUS, PLUS, STAR, SLASH, PERCENT,
             LT_OP, GT_OP, XOR, OR, QUEST, LEFT_BRACKET, RIGHT_BRACKET,
      LEFT_BRACE, RIGHT_BRACE, HASH, AT, DOLLAR;

    rule<ScannerT> STRING_LITERAL_PART, STRING_LITERAL, INT_CONSTANT_HEX, 
                   INT_CONSTANT, INT_CONSTANT_OCT, INT_CONSTANT_DEC, 
                   INT_CONSTANT_CHAR, FLOAT_CONSTANT, FLOAT_CONSTANT_1, 
                   FLOAT_CONSTANT_2, FLOAT_CONSTANT_3, CONSTANT, IDENTIFIER,
                   SYMBOL_LITERAL, ERROR_LITERAL;

    rule<ScannerT> IF, ELSE, LAMBDA, NEW, RETURN, TRY, CATCH, WHILE, DO, IN,
      BREAK, CONTINUE, FOR, NAME, MY, VERB, DELEGATE, OBJECT, METHOD,
      SELF, SELECTOR, SOURCE, CALLER, ARGS, TRUE, FALSE, NOTIFY, DETACH, PASS,
      REMOVE, THROW;
    
    definition(mica_grammar const& self) :
      RIGHT_OP(">>"), LEFT_OP("<<"), INC_OP("++"), DEC_OP("--"), 
      PTR_OP("->"), AND_OP("&&"), OR_OP("||"), LE_OP("<="), GE_OP(">="), 
      ASSIGN(":="), NE_OP("!="), MAP_START("#["), LEFT_ASSOC("<="), 
      RIGHT_ASSOC("=>"), SEMICOLON(';'), COMMA(','), COLON(':'), EQ_OP('='), 
      LEFT_PAREN('('),  RIGHT_PAREN(')'), DOT('.'), ADDROF('&'), BANG('!'), 
      TILDE('~'), MINUS('-'), PLUS('+'), STAR('*'), SLASH('/'), 
      PERCENT('%'), LT_OP('<'), GT_OP('>'), XOR('^'), OR('|'), QUEST('?'),
      LEFT_BRACKET('['), RIGHT_BRACKET(']'), LEFT_BRACE('{'), RIGHT_BRACE('}'),
      HASH('#'), AT('@'), DOLLAR('$')
    {
      keywords = "if", "else", "lambda", "new", "return", "new", "verb",
	"delegate", "my", "while", "try", "catch", "break", "continue", "name",
	"object", "method", "do", "until", "for", "in", "self", "selector",
	"source", "caller", "args", "true", "false", "notify", "detach",
	"pass", "remove", "throw";

      IF = strlit<>("if");
      ELSE = strlit<>("else");
      LAMBDA = strlit<>("lambda");
      NEW = strlit<>("new");
      RETURN = strlit<>("return");
      TRY = strlit<>("try");
      CATCH = strlit<>("catch");
      WHILE = strlit<>("while");
      FOR = strlit<>("for");
      DO = strlit<>("do");
      IN = strlit<>("in");
      BREAK = strlit<>("break");
      CONTINUE = strlit<>("continue");
      VERB = strlit<>("verb");
      DELEGATE = strlit<>("delegate");
      MY = strlit<>("my");
      NAME = strlit<>("name");
      OBJECT = strlit<>("object");
      METHOD = strlit<>("method");
      THROW = strlit<>("throw");
      REMOVE = strlit<>("remove");

      PASS = strlit<>("pass");
      SELF = strlit<>("self");
      SELECTOR = strlit<>("selector");
      SOURCE = strlit<>("source");
      CALLER = strlit<>("caller");
      ARGS = strlit<>("args");
      TRUE = strlit<>("true");
      FALSE = strlit<>("false");
      NOTIFY = strlit<>("notify");
      DETACH = strlit<>("detach");

      // identifiers
      IDENTIFIER =
	lexeme_d[
		 ((alpha_p | '_' | '$') >> *(alnum_p | '_' | '$'))
		 - (keywords >> anychar_p - (alnum_p | '_' | '$'))
	]
	;

      // symbol
      SYMBOL_LITERAL =
	HASH >> IDENTIFIER;

      // error
      ERROR_LITERAL =
	TILDE >> IDENTIFIER >> !(LEFT_PAREN >> STRING_LITERAL >> RIGHT_PAREN);

      // string literals
      STRING_LITERAL_PART =
	lexeme_d[
		 !chlit<>('L') >> chlit<>('\"') >>
		 *( strlit<>("\\\"") | anychar_p - chlit<>('\"') ) >>
		 chlit<>('\"')
	]
	;

      STRING_LITERAL = +STRING_LITERAL_PART;

      // integer constants
      INT_CONSTANT_HEX
	= lexeme_d[
		   chlit<>('0')
		   >> as_lower_d[chlit<>('x')]
		   >> +xdigit_p
		   >> !as_lower_d[chlit<>('l') | chlit<>('u')]
	]
	;

      INT_CONSTANT_OCT
	= lexeme_d[
		   chlit<>('0')
		   >> +range<>('0', '7')
		   >> !as_lower_d[chlit<>('l') | chlit<>('u')]
	]
	;

      INT_CONSTANT_DEC
	= lexeme_d[
		   +digit_p
		   >> !as_lower_d[chlit<>('l') | chlit<>('u')]
	]
	;

      INT_CONSTANT_CHAR
	= lexeme_d[
		   !chlit<>('L') >> chlit<>('\'') >>
		   longest_d[
			     anychar_p
			     |   (   chlit<>('\\')
				     >> chlit<>('0')
				     >> repeat_p(0, 2)[range<>('0', '7')]
				     )
			     |   (chlit<>('\\') >> anychar_p)
		   ] >>
		   chlit<>('\'')
	]
	;

      INT_CONSTANT =
	INT_CONSTANT_HEX
	|   INT_CONSTANT_OCT
	|   INT_CONSTANT_DEC
	|   INT_CONSTANT_CHAR
	;

      // float constants
      FLOAT_CONSTANT_1    // 12345[eE][+-]123[lLfF]?
	= lexeme_d[
		   +digit_p
		   >> (chlit<>('e') | chlit<>('E'))
		   >> !(chlit<>('+') | chlit<>('-'))
		   >> +digit_p
		   >> !as_lower_d[chlit<>('l') | chlit<>('f')]
	]
	;

      FLOAT_CONSTANT_2    // .123([[eE][+-]123)?[lLfF]?
	= lexeme_d[
		   *digit_p
		   >> chlit<>('.')
		   >> +digit_p
		   >> !(   (chlit<>('e') | chlit<>('E'))
			   >> !(chlit<>('+') | chlit<>('-'))
			   >> +digit_p
			   )
		   >> !as_lower_d[chlit<>('l') | chlit<>('f')]
	]
	;

      FLOAT_CONSTANT_3    // 12345.([[eE][+-]123)?[lLfF]?
	= lexeme_d[
		   +digit_p
		   >> chlit<>('.')
		   >> *digit_p
		   >> !(   (chlit<>('e') | chlit<>('E'))
			   >> !(chlit<>('+') | chlit<>('-'))
			   >> +digit_p
			   )
		   >> !as_lower_d[chlit<>('l') | chlit<>('f')]
	]
	;

      FLOAT_CONSTANT
	= FLOAT_CONSTANT_1
	| FLOAT_CONSTANT_2
	| FLOAT_CONSTANT_3
	;

      CONSTANT = longest_d[FLOAT_CONSTANT | INT_CONSTANT];

      literals 
	= SELF | SELECTOR | SOURCE | CALLER | ARGS | TRUE | FALSE;

      primary_expression 
	= CONSTANT
	| STRING_LITERAL
	| SYMBOL_LITERAL
	| ERROR_LITERAL
	| literals
	| list_expression
	| map_expression
	| lambda_expression
	| method
	| object
	| message
	| declare
	| control_statement
	| slot_or_var
	| LEFT_PAREN >> expression >> RIGHT_PAREN
	;

      postfix_expression
	= primary_expression >> postfix_expression_helper
	;

      postfix_expression_helper
	=   (   (LEFT_BRACKET >> expression >> RIGHT_BRACKET)
		|  LEFT_PAREN >> !argument_expression_list >> RIGHT_PAREN)
			      >> postfix_expression_helper
	| epsilon_p
	;

      argument_expression_list
	= assignment_expression >> *(COMMA >> assignment_expression)
	;
      
      multiplicative_expression
	= postfix_expression >> multiplicative_expression_helper
	;

      multiplicative_expression_helper
	=   (   SLASH >> postfix_expression
	      | PERCENT >> postfix_expression
	     ) >> multiplicative_expression_helper
	| epsilon_p
	;

      additive_expression
	= multiplicative_expression >> additive_expression_helper
	;

      additive_expression_helper
	=   (  PLUS >> multiplicative_expression
	     | MINUS >> multiplicative_expression ) >>
	additive_expression_helper
	| epsilon_p
	;

      shift_expression
	= additive_expression >> shift_expression_helper
	;

      shift_expression_helper
	=   (
	     LEFT_OP >> additive_expression
	     |   RIGHT_OP >> additive_expression
	     ) >> shift_expression_helper
	| epsilon_p
	;

      relational_expression
	= shift_expression >> relational_expression_helper
	;

      relational_expression_helper
	=   (
	     LT_OP >> shift_expression
	     |   GT_OP >> shift_expression
	     |   LE_OP >> shift_expression
	     |   GE_OP >> shift_expression
	     ) >> relational_expression_helper
	| epsilon_p
	;

      equality_expression
	= relational_expression >> equality_expression_helper
	;

      equality_expression_helper
	=   (  EQ_OP >> relational_expression
	     | NE_OP >> relational_expression ) >>
	equality_expression_helper
	| epsilon_p
	;

      and_expression
	= equality_expression >> and_expression_helper
	;

      and_expression_helper
	= ADDROF >> equality_expression >> and_expression_helper
	| epsilon_p
	;

      exclusive_or_expression
	= and_expression >> exclusive_or_expression_helper
	;

      exclusive_or_expression_helper
	= XOR >> and_expression >> exclusive_or_expression_helper
	| epsilon_p
	;

      inclusive_or_expression
	= exclusive_or_expression >> inclusive_or_expression_helper
	;

      inclusive_or_expression_helper
	= OR >> exclusive_or_expression >> inclusive_or_expression_helper
	| epsilon_p
	;

      logical_and_expression
	= inclusive_or_expression >> logical_and_expression_helper
	;

      logical_and_expression_helper
	= AND_OP >> inclusive_or_expression >> logical_and_expression_helper
	| epsilon_p
	;

      logical_or_expression
	= logical_and_expression >> logical_or_expression_helper
	;

      logical_or_expression_helper
	= OR_OP >> logical_and_expression >> logical_or_expression_helper
	| epsilon_p
	;

      assignment_expression
	= postfix_expression >> ASSIGN >> assignment_expression
	| postfix_expression >> RIGHT_ASSOC
			     >> !(NEW) >> LEFT_PAREN >> argument_mask >> RIGHT_PAREN
	| logical_or_expression
	;

      expression = 
	assignment_expression >> expression_helper
	;

      expression_helper
	= SEMICOLON >> assignment_expression >> expression_helper
	| epsilon_p
	;

      do_loop = DO >> loop_statement >> 
	WHILE >> LEFT_PAREN >> expression >> RIGHT_PAREN;

      for_range = FOR >> IDENTIFIER >> IN >> expression >> DO >> 
	loop_statement;

      while_loop = WHILE >> LEFT_PAREN >> expression >> RIGHT_PAREN >>
	loop_statement;

      catch_block = CATCH >> LEFT_PAREN >>
	IDENTIFIER >> ASSIGN >> ERROR_LITERAL >> 
	RIGHT_PAREN >> statement;

      try_catch =
	TRY >> statement >> +(catch_block);

      message =
	+(IDENTIFIER >> COLON >> statement);

      list_expression =
	LEFT_BRACKET >> *(list_p( expression, COMMA)) >> RIGHT_BRACKET;

      map_item =
	expression >> RIGHT_ASSOC >> expression;
     
      argument_declaration = LT_OP >> argument_mask >> GT_OP;

      mandatory = list_p( slot_or_var, COMMA );
      optional  = list_p( QUEST >> slot_or_var, COMMA );
      remainder = list_p( AT >> slot_or_var, COMMA );

      argument_mask 
	=  !(             mandatory) 
	>> !( !(COMMA) >> optional)
	>> !( !(COMMA) >> remainder);

      map_expression =
	MAP_START >> *(list_p( map_item, COMMA)) >> RIGHT_BRACKET;

      declare = 
	NEW >> slot_or_var >> !(ASSIGN >> expression);

      slot_or_var =
	IDENTIFIER |
	(MY | DOT) >> IDENTIFIER |
	(NAME | DOLLAR) >> IDENTIFIER |
	DELEGATE >> IDENTIFIER |
	VERB >> message;
		     
      loop_statement
	= statement
	| BREAK >> SEMICOLON
	| CONTINUE >> SEMICOLON;
      
      lambda_expression =
	LAMBDA >> argument_declaration >> compound_statement;

      method =
	METHOD >> argument_declaration >> compound_statement;

      object = 
	OBJECT >> compound_statement;

      compound_statement =
	LEFT_BRACE >> +statement >> RIGHT_BRACE;

      if_expression =
	IF >> LEFT_PAREN >> expression >> RIGHT_PAREN >> 
	statement >> !(ELSE >> statement);

      control_statement
	= if_expression
	| try_catch
	| while_loop
	| do_loop
	| for_range
	| compound_statement;

      statement 
	= expression >> !(SEMICOLON)
	| RETURN >> expression >> SEMICOLON
	| NOTIFY >> LEFT_PAREN >> expression >> RIGHT_PAREN >> SEMICOLON
	| DETACH >> LEFT_PAREN >> RIGHT_PAREN >> SEMICOLON
	| THROW >> ERROR_LITERAL >> SEMICOLON
	| REMOVE >> list_p( slot_or_var, COMMA ) >> SEMICOLON;

    }

    rule<ScannerT> const start() const { return statement; }

  };
};

namespace mica {
  NPtr compile_nodes( const tree_parse_info<> &info ) {
    
  }
}

void compile_it()
{
  mica_grammar grammar;

  string str;
  while (getline(cin, str)) {
    try {
      tree_parse_info<> info = pt_parse( str.c_str(), grammar, space_p);

      if (info.full) {
	cout << "parsing succeeded\n";

	mica::compile_nodes( info );
      } else {
	cout << "parsing failed\n";
      }
    } catch (...) {
      cout << "error during parse" << endl;
    }
  }

}



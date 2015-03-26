// #define BOOST_SPIRIT_DEBUG

#include <boost/spirit/include/classic_ast.hpp>
#include <boost/spirit/include/classic_attribute.hpp>
#include <boost/spirit/include/classic_core.hpp>
#include <boost/spirit/include/classic_functor_parser.hpp>
#include <boost/spirit/include/classic_parse_tree.hpp>
#include <boost/spirit/include/classic_position_iterator.hpp>
#include <boost/spirit/include/classic_symbols.hpp>
#include <boost/spirit/include/classic_symbols.hpp>
#include <boost/spirit/include/classic_tree_to_xml.hpp>
#include <boost/spirit/include/classic_utility.hpp>
#include <fstream>
#include <iostream>
#include <string>
#include <wchar.h>

#include "types/Var.hh"
#include "types/hash.hh"
#include "types/Symbol.hh"
#include "types/List.hh"
#include "types/OpCode.hh"
#include "types/MetaObjects.hh"
#include "types/String.hh"
#include "vm/Block.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "Nodes.hh"

using namespace std;
using namespace boost::spirit::classic;
using namespace mica;

/** Here's some convenient typedefs
 */
typedef char const* iterator_t;
typedef node_val_data_factory<Var> factory_t;
typedef tree_node<factory_t> tree_node_t;
typedef tree_match<iterator_t, factory_t> match_t;
typedef ast_match_policy<iterator_t, factory_t> match_policy_t;
typedef scanner<iterator_t, scanner_policies<iter_policy_t, match_policy_t> > scanner_t;
typedef match_t::tree_iterator iter_t;
typedef match_t::const_tree_iterator const_iter_t;

#define RULE(x) rule<scanner_t, parser_context<nil_t>, parser_tag<parser_ids::x> > x

struct parser_ids {
  typedef enum {
    list_expression,
    primary_expression,
    map_expression,
    map_item,
    statement,
    if_expression,
    compound_statement,
    lambda_expression,
    assignment,
    declare,
    message,
    while_loop,
    do_loop,
    for_range,
    try_catch,
    catch_block,
    loop_statement,
    control_statement,
    slot_or_var,
    argument_mask,
    argument_declaration,
    method,
    object,
    literals,

    mandatory,
    optional,
    remainder,

    postfix_expression,
    multiplicative_expression,
    additive_expression,
    shift_expression,
    relational_expression,
    equality_expression,
    and_expression,
    exclusive_or_expression,
    inclusive_or_expression,
    logical_and_expression,
    logical_or_expression,
    assignment_expression,
    argument_expression_list,
    assignment_operator,
    expression,
    program,

    NUMBER,
    HEX,
    FLOAT,
    IDENTIFIER,
    STRING_LITERAL,
    SYMBOL_LITERAL,
    ERROR_LITERAL,

    IF,
    ELSE,
    LAMBDA,
    NEW,
    RETURN,
    TRY,
    CATCH,
    WHILE,
    DO,
    IN,
    BREAK,
    CONTINUE,
    FOR,
    NAME,
    MY,
    VERB,
    DELEGATE,
    OBJECT,
    METHOD,
    SELF,
    SELECTOR,
    SOURCE,
    CALLER,
    ARGS,
    TRUE,
    FALSE,
    NOTIFY,
    DETACH,
    PASS,
    REMOVE,
    THROW

  } parser_ids_enum;
};

/** Converts a string literal into a mica String
 */
struct assign_string {
  void operator()(tree_node<node_val_data<const char*, mica::Var> >& n, const char*& b,
                  const char* const& e) const {
    size_t length = e - b - 1;  // (chop off quotes)
    mica_string r;
    for (unsigned int i = 1; i < length; i++) {
      char c;
      parse(b + i, c_escape_ch_p[assign(c)]);
      if (b[i] == '\\')
        b++;

      r.push_back(c);
    }
    n.value.value(Var(String::from_rope(r)));
  }
};

struct assign_integer {
  void operator()(tree_node<node_val_data<const char*, mica::Var> >& n, const char*& b,
                  const char* const& e) const {
    int number;
    parse(b, int_p[assign(number)]);
    cerr << "INT: " << number << " " << typeid(number).name() << endl;
    n.value.value(Var(number));
  }
};

struct assign_float {
  void operator()(tree_node<node_val_data<const char*, mica::Var> >& n, const char*& b,
                  const char* const& e) const {
    float number;
    parse(b, real_p[assign(number)]);
    cerr << "FLOAT: " << number << " " << typeid(number).name() << endl;
    n.value.value(Var(number));
  }
};

struct assign_hex {
  void operator()(tree_node<node_val_data<const char*, mica::Var> >& n, const char*& b,
                  const char* const& e) const {
    int number;
    parse(b, hex_p[assign(number)]);
    cerr << "HEX: " << number << " " << typeid(number).name() << endl;
    n.value.value(Var(number));
  }
};

struct assign_symbol {
  void operator()(tree_node<node_val_data<const char*, mica::Var> >& n, const char*& b,
                  const char* const& e) const {
    mica_string x(b + 1, e);  // +1 to eliminate # at beginning

    cerr << "sym: " << x << endl;
    n.value.value(Var(Symbol::create(x)));
  }
};

struct mica_grammar {
  /** All of these get declared in global scope so that we can use parse_id
   *  on them during compilation
   */
  RULE(list_expression);
  RULE(primary_expression);
  RULE(map_expression);
  RULE(map_item);
  RULE(statement);
  RULE(if_expression);
  RULE(compound_statement);
  RULE(lambda_expression);
  RULE(assignment);
  RULE(declare);
  RULE(message);
  RULE(while_loop);
  RULE(do_loop);
  RULE(for_range);
  RULE(try_catch);
  RULE(catch_block);
  RULE(loop_statement);
  RULE(control_statement);
  RULE(slot_or_var);
  RULE(argument_mask);
  RULE(argument_declaration);
  RULE(method);
  RULE(object);
  RULE(literals);

  RULE(mandatory);
  RULE(optional);
  RULE(remainder);

  RULE(postfix_expression);
  RULE(multiplicative_expression);
  RULE(additive_expression);
  RULE(shift_expression);
  RULE(relational_expression);
  RULE(equality_expression);
  RULE(and_expression);
  RULE(exclusive_or_expression);
  RULE(inclusive_or_expression);
  RULE(logical_and_expression);
  RULE(logical_or_expression);
  RULE(assignment_expression);
  RULE(argument_expression_list);
  RULE(assignment_operator);
  RULE(expression);
  RULE(program);

  RULE(NUMBER);
  RULE(HEX);
  RULE(FLOAT);
  RULE(IDENTIFIER);
  RULE(STRING_LITERAL);
  RULE(SYMBOL_LITERAL);
  RULE(ERROR_LITERAL);

  RULE(IF);
  RULE(ELSE);
  RULE(LAMBDA);
  RULE(NEW);
  RULE(RETURN);
  RULE(TRY);
  RULE(CATCH);
  RULE(WHILE);
  RULE(DO);
  RULE(IN);
  RULE(BREAK);
  RULE(CONTINUE);
  RULE(FOR);
  RULE(NAME);
  RULE(MY);
  RULE(VERB);
  RULE(DELEGATE);
  RULE(OBJECT);
  RULE(METHOD);
  RULE(SELF);
  RULE(SELECTOR);
  RULE(SOURCE);
  RULE(CALLER);
  RULE(ARGS);
  RULE(TRUE);
  RULE(FALSE);
  RULE(NOTIFY);
  RULE(DETACH);
  RULE(PASS);
  RULE(REMOVE);
  RULE(THROW);

  symbols<> keywords;

  strlit<> RIGHT_OP, LEFT_OP, INC_OP, DEC_OP, PTR_OP, AND_OP, OR_OP, LE_OP, GE_OP, ASSIGN, NE_OP,
      MAP_START, LEFT_ASSOC, RIGHT_ASSOC;

  chlit<> SEMICOLON, COMMA, COLON, EQ_OP, LEFT_PAREN, RIGHT_PAREN, DOT, ADDROF, BANG, TILDE, MINUS,
      PLUS, STAR, SLASH, PERCENT, LT_OP, GT_OP, XOR, OR, QUEST, LEFT_BRACKET, RIGHT_BRACKET,
      LEFT_BRACE, RIGHT_BRACE, HASH, AT, DOLLAR, QUOTE;

  mica_grammar()
      : RIGHT_OP(">>"),
        LEFT_OP("<<"),
        INC_OP("++"),
        DEC_OP("--"),
        PTR_OP("->"),
        AND_OP("&&"),
        OR_OP("||"),
        LE_OP("<="),
        GE_OP(">="),
        ASSIGN(":="),
        NE_OP("!="),
        MAP_START("#["),
        LEFT_ASSOC("<="),
        RIGHT_ASSOC("=>"),
        SEMICOLON(';'),
        COMMA(','),
        COLON(':'),
        EQ_OP('='),
        LEFT_PAREN('('),
        RIGHT_PAREN(')'),
        DOT('.'),
        ADDROF('&'),
        BANG('!'),
        TILDE('~'),
        MINUS('-'),
        PLUS('+'),
        STAR('*'),
        SLASH('/'),
        PERCENT('%'),
        LT_OP('<'),
        GT_OP('>'),
        XOR('^'),
        OR('|'),
        QUEST('?'),
        LEFT_BRACKET('['),
        RIGHT_BRACKET(']'),
        LEFT_BRACE('{'),
        RIGHT_BRACE('}'),
        HASH('#'),
        AT('@'),
        DOLLAR('$'),
        QUOTE('"') {
    keywords = "if", "else", "lambda", "new", "return", "new", "verb", "delegate", "my", "while",
    "try", "catch", "break", "continue", "name", "object", "method", "do", "until", "for", "in",
    "self", "selector", "source", "caller", "args", "true", "false", "notify", "detach", "pass",
    "remove", "throw";

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

    // string
    STRING_LITERAL = inner_node_d[confix_p('"', (*c_escape_ch_p), '"')];

    // identifer
    IDENTIFIER =
        lexeme_d[(alpha_p >> *(alnum_p | '_')) - (keywords >> anychar_p - (alnum_p | '_'))];
    ;

    // symbol
    SYMBOL_LITERAL = access_node_d[HASH >> IDENTIFIER][assign_symbol()];

    // error
    ERROR_LITERAL = discard_node_d[TILDE] >> access_node_d[IDENTIFIER][assign_symbol()] >>
        !(inner_node_d[LEFT_PAREN >> STRING_LITERAL >> RIGHT_PAREN]);

    NUMBER = access_node_d[int_p][assign_integer()];

    HEX = strlit<>("0x") >> access_node_d[hex_p][assign_hex()];

    FLOAT = access_node_d[real_p][assign_float()];

    literals = SELF | SELECTOR | SOURCE | CALLER | ARGS | TRUE | FALSE;

    assignment_expression =
        slot_or_var >> *((root_node_d[ASSIGN] >> expression) |
                         (primary_expression >> root_node_d[RIGHT_ASSOC] >> !(NEW) >>
                          inner_node_d[LEFT_PAREN >> argument_mask >> RIGHT_PAREN]));

    primary_expression = longest_d[NUMBER | FLOAT] | HEX |
        access_node_d[STRING_LITERAL][assign_string()] | SYMBOL_LITERAL | ERROR_LITERAL |
        assignment_expression | leaf_node_d[literals] | list_expression | map_expression |
        lambda_expression | method | object | message | declare | control_statement |
        inner_node_d[LEFT_PAREN >> expression >> RIGHT_PAREN];

    postfix_expression = primary_expression >>
        *((LEFT_BRACKET >> expression >> discard_node_d[RIGHT_BRACKET]) |
          (inner_node_d[LEFT_PAREN >> !argument_expression_list >> RIGHT_PAREN]));

    argument_expression_list = infix_node_d[expression >> *(COMMA >> expression)];

    multiplicative_expression = postfix_expression >>
        *((root_node_d[SLASH] >> postfix_expression) |
          (root_node_d[PERCENT] >> postfix_expression) | (root_node_d[STAR] >> postfix_expression));

    additive_expression =
        multiplicative_expression >> *((root_node_d[PLUS] >> multiplicative_expression) |
                                       root_node_d[MINUS] >> multiplicative_expression);

    shift_expression = additive_expression >> *((root_node_d[LEFT_OP] >> additive_expression) |
                                                (root_node_d[RIGHT_OP] >> additive_expression));

    relational_expression = shift_expression >>
        *((root_node_d[LT_OP] >> shift_expression) | (root_node_d[GT_OP] >> shift_expression) |
          (root_node_d[LE_OP] >> shift_expression) | (root_node_d[GE_OP] >> shift_expression));

    equality_expression = relational_expression >> *((root_node_d[EQ_OP] >> relational_expression) |
                                                     (root_node_d[NE_OP] >> relational_expression));

    and_expression = equality_expression >> *(root_node_d[ADDROF] >> equality_expression);

    exclusive_or_expression = and_expression >> *(root_node_d[XOR] >> and_expression);

    inclusive_or_expression =
        exclusive_or_expression >> *(root_node_d[OR] >> exclusive_or_expression);

    logical_and_expression =
        inclusive_or_expression >> *(root_node_d[AND_OP] >> inclusive_or_expression);

    logical_or_expression =
        logical_and_expression >> *(root_node_d[OR_OP] >> logical_and_expression);

    expression = logical_or_expression;

    do_loop = root_node_d[DO] >> loop_statement >> discard_node_d[WHILE] >>
        inner_node_d[LEFT_PAREN >> expression >> RIGHT_PAREN];

    for_range = root_node_d[FOR] >> IDENTIFIER >> discard_node_d[IN] >> expression >>
        discard_node_d[DO] >> loop_statement;

    while_loop = WHILE >> inner_node_d[LEFT_PAREN >> expression >> RIGHT_PAREN] >> loop_statement;

    catch_block = root_node_d[CATCH] >>
        inner_node_d[LEFT_PAREN >> IDENTIFIER >> discard_node_d[ASSIGN] >> ERROR_LITERAL >>
                     RIGHT_PAREN] >>
        statement;

    try_catch = TRY >> statement >> +(catch_block);

    message = +(IDENTIFIER >> COLON >> statement);

    list_expression =
        inner_node_d[LEFT_BRACKET >> infix_node_d[*(list_p(expression, COMMA))] >> RIGHT_BRACKET];

    map_item = expression >> discard_node_d[RIGHT_ASSOC] >> expression;

    map_expression = inner_node_d[MAP_START >> *(list_p(map_item, COMMA)) >> RIGHT_BRACKET];

    argument_declaration = inner_node_d[LT_OP >> argument_mask >> GT_OP];

    mandatory = infix_node_d[list_p(slot_or_var, COMMA)];
    optional = infix_node_d[(QUEST >> slot_or_var, COMMA)];
    remainder = infix_node_d[(AT >> slot_or_var, COMMA)];

    argument_mask = !(mandatory) >> !(!(COMMA) >> optional) >> !(!(COMMA) >> remainder);

    declare = NEW >> slot_or_var >> !(ASSIGN >> expression);

    slot_or_var = IDENTIFIER | (MY | DOT) >> IDENTIFIER | (NAME | DOLLAR) >> IDENTIFIER |
        DELEGATE >> IDENTIFIER | VERB >> message;

    loop_statement = statement | BREAK >> SEMICOLON | CONTINUE >> SEMICOLON;

    lambda_expression = LAMBDA >> argument_declaration >> compound_statement;

    method = METHOD >> argument_declaration >> compound_statement;

    object = OBJECT >> compound_statement;

    compound_statement = inner_node_d[LEFT_BRACE >> +statement >> RIGHT_BRACE];

    if_expression = root_node_d[IF] >> inner_node_d[LEFT_PAREN >> expression >> RIGHT_PAREN] >>
        statement >> !(root_node_d[ELSE] >> statement);

    control_statement =
        if_expression | try_catch | while_loop | do_loop | for_range | compound_statement;

    statement = expression >> !SEMICOLON | root_node_d[RETURN] >> expression >> SEMICOLON |
        root_node_d[NOTIFY] >> LEFT_PAREN >> expression >> RIGHT_PAREN >> SEMICOLON |
        root_node_d[DETACH] >> LEFT_PAREN >> RIGHT_PAREN >> SEMICOLON |
        root_node_d[THROW] >> ERROR_LITERAL >> SEMICOLON |
        root_node_d[REMOVE] >> list_p(slot_or_var, COMMA) >> SEMICOLON;

    program = (*statement);
  }

  match_t parse(const std::string& str) {
    const char* first = str.c_str();
    const char* last = first + str.size();

    scanner_t scan = scanner_t(first, last);
    match_t info = program.parse(scan);

    return info;
  }
};

NPtr compile_statement(iter_t& i) {
  NPtr result;

  /** handle literals
   */
  if (i->value.id() == parser_ids::SYMBOL_LITERAL) {
    cerr << "FOUND SYMBOL: " << i->children.begin()->value.value() << endl;
  } else if (i->value.id() == parser_ids::STRING_LITERAL) {
    cerr << "FOUND STRING: " << i->value.value() << endl;
  } else if (i->value.id() == parser_ids::FLOAT) {
    cerr << "FOUND FLOAT: " << i->value.value() << endl;
  } else if (i->value.id() == parser_ids::NUMBER) {
    cerr << "FOUND INTEGER: " << i->value.value() << endl;
  } else {
    for (iter_t j = i->children.begin(); j != i->children.end(); j++) compile_statement(j);
  }
  return result;
}

void compile_nodes(const match_t& info) {
  tree_to_xml(cout, info.trees);

  std::vector<NPtr> block;
  for (iter_t i = info.trees.begin(); i != info.trees.end(); i++)
    block.push_back(compile_statement(i));
}

void compile_it() {
  mica_grammar grammar;

  std::string str;
  while (getline(cin, str)) {
    try {
      match_t info = grammar.parse(str);

      //      if (info.full) {
      //	cout << "parsing succeeded\n";

      compile_nodes(info);
      //      } else {
      //	cout << "parsing failed\n";
      //      }
    } catch (...) {
      cout << "error during parse" << endl;
    }
  }
}

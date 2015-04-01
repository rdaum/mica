#include <cassert>
#include <functional>
#include <iostream>
#include <map>
#include <pegtl.hh>
#include <pegtl/analyze.hh>
#include <pegtl/trace.hh>
#include <pegtl/contrib/raw_string.hh>

#include <pegtl/read_parser.hh>

using namespace pegtl;

namespace mica {
namespace grammar {

struct sep : pegtl::sor<pegtl::ascii::space> {};
struct seps : pegtl::star<sep> {};

struct str_break : pegtl::string<'b', 'r', 'e', 'a', 'k'> {};
struct str_catch : pegtl::string<'c', 'a', 't', 'c', 'h'> {};
struct str_continue : pegtl::string<'c', 'o', 'n', 't', 'i', 'n', 'u', 'e'> {};
struct str_do : pegtl::string<'d', 'o'> {};
struct str_new : pegtl::string<'n', 'e', 'w'> {};
struct str_delegate : pegtl::string<'d', 'e', 'l', 'e', 'g', 'a', 't', 'e'> {};
struct str_else : pegtl::string<'e', 'l', 's', 'e'> {};
struct str_for : pegtl::string<'f', 'o', 'r'> {};
struct str_if : pegtl::string<'i', 'f'> {};
struct str_in : pegtl::string<'i', 'n'> {};
struct str_lambda : pegtl::string<'l', 'a', 'm', 'b', 'd', 'a'> {};
struct str_method : pegtl::string<'m', 'e', 't', 'h', 'o', 'd'> {};
struct str_name : pegtl::string<'n', 'a', 'm', 'e'> {};
struct str_my : pegtl::string<'m', 'y'> {};
struct str_like : pegtl::string<'l', 'i', 'k', 'e'> {};
struct str_remove : pegtl::string<'r', 'e', 'm', 'o', 'v', 'e'> {};
struct str_return : pegtl::string<'r', 'e', 't', 'u', 'r', 'n'> {};
struct str_slots : pegtl::string<'s', 'l', 'o', 't', 's'> {};
struct str_self : pegtl::string<'s', 'e', 'l', 'f'> {};
struct str_throw : pegtl::string<'t', 'h', 'r', 'o', 'w'> {};
struct str_notify : pegtl::string<'n', 'o', 't', 'i', 'f', 'y'> {};
struct str_detach : pegtl::string<'d', 'e', 't', 'a', 'c', 'h'> {};
struct str_selector : pegtl::string<'s', 'e', 'l', 'e', 'c', 't', 'o', 'r'> {};
struct str_source : pegtl::string<'s', 'o', 'u', 'r', 'c', 'e'> {};
struct str_caller : pegtl::string<'c', 'a', 'l', 'l', 'e', 'r'> {};
struct str_args : pegtl::string<'a', 'r', 'g', 's'> {};
struct str_try : pegtl::string<'t', 'r', 'y'> {};
struct str_verb : pegtl::string<'v', 'e', 'r', 'b'> {};
struct str_while : pegtl::string<'w', 'h', 'i', 'l', 'e'> {};
struct str_object : pegtl::string<'o', 'b', 'j', 'e', 'c', 't'> {};
struct str_true : pegtl::string<'t', 'r', 'u', 'e'> {};
struct str_false : pegtl::string<'f', 'a', 'l', 's', 'e'> {};
struct str_map : pegtl::string<'m', 'a', 'p'> {};
struct str_and : pegtl::string<'&', '&'> {};
struct str_or : pegtl::string<'|', '|'> {};

struct token_colon : pegtl::one<':'> {};

struct token_semicolon : pegtl::one<';'> {};

struct token_lparen : pegtl::one<'('> {};

struct token_rparen : pegtl::one<')'> {};

// String literals
struct single : pegtl::one<'a', 'b', 'f', 'n', 'r', 't', 'v', '\\', '"', '\'', '0', '\n'> {};
struct spaces : pegtl::seq<pegtl::one<'z'>, pegtl::star<pegtl::space>> {};
struct hexbyte : pegtl::if_must<pegtl::one<'x'>, pegtl::xdigit, pegtl::xdigit> {};
struct decbyte : pegtl::if_must<pegtl::digit, pegtl::rep_opt<2, pegtl::digit>> {};
struct unichar : pegtl::if_must<pegtl::one<'u'>, pegtl::one<'{'>, pegtl::plus<pegtl::xdigit>,
                                pegtl::one<'}'>> {};
struct escaped
    : pegtl::if_must<pegtl::one<'\\'>, pegtl::sor<hexbyte, decbyte, unichar, single, spaces>> {};
struct regular : pegtl::not_one<'\r', '\n'> {};
struct character : pegtl::sor<escaped, regular> {};
struct string_literal : pegtl::if_must<pegtl::one<'"'>, pegtl::until<pegtl::one<'"'>, character>> {
};

// Numeric literals
template <typename E>
struct exponent
    : pegtl::opt<pegtl::if_must<E, pegtl::opt<pegtl::one<'+', '-'>>, pegtl::plus<pegtl::digit>>> {};

template <typename D, typename E>
struct numeral_three : pegtl::seq<pegtl::if_must<pegtl::one<'.'>, pegtl::plus<D>>, exponent<E>> {};
template <typename D, typename E>
struct numeral_two
    : pegtl::seq<pegtl::plus<D>, pegtl::opt<pegtl::one<'.'>, pegtl::star<D>>, exponent<E>> {};
template <typename D, typename E>
struct numeral_one : pegtl::sor<numeral_two<D, E>, numeral_three<D, E>> {};

struct decimal : numeral_one<pegtl::digit, pegtl::one<'e', 'E'>> {};
struct hexadecimal
    : pegtl::if_must<pegtl::istring<'0', 'x'>, numeral_one<pegtl::xdigit, pegtl::one<'p', 'P'>>> {};

struct number_literal : pegtl::sor<hexadecimal, decimal> {};

// Forward declare this for later use.
struct expression;

// Atoms
struct boolean_literal : sor<str_true, str_false> {};

struct symbol_literal : if_must<pegtl::one<'\''>, identifier> {};

struct literals : sor<number_literal, symbol_literal, string_literal, boolean_literal> {};

struct builtin_values : sor<str_self, str_selector, str_source, str_caller, str_args, str_slots> {};

// Slots

// Slot name format
struct slot_name_expr : seq<token_lparen, seps, expression, seps, token_rparen> {};
struct slot_name : sor<identifier, slot_name_expr> {};

// Private ivar slots
struct private_slot_ref : sor<str_my, pegtl::one<'.'>> {};
struct private_slot : seq<private_slot_ref, seps, slot_name> {};

// Global names
struct name_slot_ref : sor<str_name, pegtl::one<'$'>> {};
struct name_slot : seq<name_slot_ref, seq<seps, slot_name>> {};

// Delegate slots
struct delegate_slot : if_must<str_delegate, seq<seps, slot_name>> {};

// Verb references
struct verb_arg : sor<expression, pegtl::one<'*'>> {};
struct verb_template_rest : list<seq<identifier, seps, token_colon, seps, verb_arg>, seps> {};
struct verb_template_root : seq<identifier, seps, opt<seq<token_colon>, seps, verb_arg>> {};
struct verb_template : seq<verb_template_root, seps, opt<verb_template_rest>> {};
struct verb_slot : if_must<str_verb, seq<seps, verb_template>> {};

struct slot_reference : sor<verb_slot, private_slot, delegate_slot, name_slot> {};

// Message send.
struct selector_key : seq<identifier, token_colon> {};

struct message_argument : if_must<selector_key, seq<seps, expression>> {};

struct first_message_argument : seq<identifier, seps, opt<token_colon>, seps, expression> {};

struct message_args : seq<first_message_argument, seps, opt<list<message_argument, seps>>> {};

struct receiver_cast : seq<expression, sep, str_like, sep, expression> {};

struct receiver_expression : sor<receiver_cast, expression> {};

struct message_expr
    : if_must<token_lparen,
              until<token_rparen, seps, receiver_expression, seps, message_args, seps>> {};

//

template <char O, char... N>
struct op_one : pegtl::seq<pegtl::one<O>, pegtl::at<pegtl::not_one<N...>>> {};
template <char O, char P, char... N>
struct op_two : pegtl::seq<pegtl::string<O, P>, pegtl::at<pegtl::not_one<N...>>> {};

template <typename S, typename O>
struct left_assoc : pegtl::seq<S, seps, pegtl::star<pegtl::if_must<O, seps, S, seps>>> {};
template <typename S, typename O>
struct right_assoc : pegtl::seq<S, seps, pegtl::opt<pegtl::if_must<O, seps, right_assoc<S, O>>>> {};

struct unary_operators
    : pegtl::sor<pegtl::one<'-'>, pegtl::one<'#'>, op_one<'~', '='>, pegtl::one<'!'>> {};

struct value;
struct expr_ten;

struct bracket_expr : seq<pegtl::one<'('>, seps, expression, seps, pegtl::one<')'>> {};

struct expr_thirteen : sor<value, bracket_expr> {};

struct expr_twelve : pegtl::sor<expr_thirteen, message_expr> {};
struct expr_eleven
    : pegtl::seq<expr_twelve, seps, pegtl::opt<pegtl::one<'^'>, seps, expr_ten, seps>> {};
struct unary_apply : pegtl::if_must<unary_operators, seps, expr_ten, seps> {};
struct expr_ten : pegtl::sor<unary_apply, expr_eleven> {};
struct operators_nine
    : pegtl::sor<pegtl::two<'/'>, pegtl::one<'/'>, pegtl::one<'*'>, pegtl::one<'%'>> {};
struct expr_nine : left_assoc<expr_ten, operators_nine> {};
struct operators_eight : pegtl::sor<pegtl::one<'+'>, pegtl::one<'-'>> {};
struct expr_eight : left_assoc<expr_nine, operators_eight> {};
struct expr_seven : right_assoc<expr_eight, op_two<'.', '.', '.'>> {};
struct operators_six : pegtl::sor<pegtl::two<'<'>, pegtl::two<'>'>> {};
struct expr_six : left_assoc<expr_seven, operators_six> {};
struct expr_five : left_assoc<expr_six, pegtl::one<'&'>> {};
struct expr_four : left_assoc<expr_five, op_one<'~', '='>> {};
struct expr_three : left_assoc<expr_four, pegtl::one<'|'>> {};
struct operators_two : pegtl::sor<pegtl::two<'='>, pegtl::string<'<', '='>, pegtl::string<'>', '='>,
                                  op_one<'<', '<'>, op_one<'>', '>'>, pegtl::string<'~', '='>> {};
struct expr_two : left_assoc<expr_three, operators_two> {};
struct expr_one : left_assoc<expr_two, str_and> {};
struct expression : left_assoc<expr_one, str_or> {};

// Expressions and statements
struct var_reference : identifier {};

struct value : sor<builtin_values, slot_reference, var_reference, literals> {};

struct expression_statement : if_must<expression, token_semicolon> {};

struct statement : sor<expression_statement, token_semicolon> {};

struct program : seq<statement> {};

// Class template for user-defined actions that does
// nothing by default.

template <typename Rule>
struct action : nothing<Rule> {};

// Specialisation of the user-defined action to do
// something when the 'name' rule succeeds; is called
// with the portion of the input that matched the rule.

template <>
struct action<number_literal> {
  static void apply(const input& in, std::string& name) { name = in.string(); }
};
template <>
struct action<string_literal> {
  static void apply(const input& in, std::string& name) { name = in.string(); }
};
template <>
struct action<symbol_literal> {
  static void apply(const input& in, std::string& name) { name = in.string(); }
};
template <>
struct action<boolean_literal> {
  static void apply(const input& in, std::string& name) { name = in.string(); }
};
template <>
struct action<var_reference> {
  static void apply(const input& in, std::string& name) {
    std::cerr << "var_reference slot: " << name << std::endl;
  }
};

}  // namespace grammar

void perform_compile() {
  analyze<mica::grammar::program>();
  std::string str;
  while (getline(std::cin, str)) {
    std::string name;
    try {
      parse<mica::grammar::program, mica::grammar::action>(str, "terminal", name);
    } catch (pegtl::parse_error& pe) {
      std::cerr << "parse error (" << pe.what() << ") at positions: " << std::endl;
      for (auto pos : pe.positions) {
        std::cerr << " line: " << pos.line << " col: " << pos.column << " begin: " << pos.begin
                  << std::endl;
      }
    }
    std::cerr << name << std::endl;
  }
}

}  // namespace mica

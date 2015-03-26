/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef MICA_NODES_HH
#define MICA_NODES_HH

#include <vector>

#include "base/Ref.hh"
#include "base/reference_counted.hh"
#include "parser/Binding.hh"
#include "types/Data.hh"
#include "types/Var.hh"

namespace mica {

using std::vector;

class Var;
class Block;
class String;

/** Parent Node class for the syntax tree.
 *
 *  Each Node defines a "compile" interface which knows how to turn
 *  itself and its children into opcodes and values for execution
 *  by the virtual machine.
 */
class Node : public reference_counted, public has_constant_type_identifier<Type::NODE> {
 private:
  friend class Ref<Node>;

 public:
  Node() : reference_counted(){};
  virtual ~Node(){};

  virtual var_vector compile(Ref<Block> block, Binding &binding) const = 0;

  virtual Ref<Block> compile_to_expr(Binding &binding, const mica_string &source = "");

  virtual void append_child_pointers(child_set &child_list);
};

class NPtr : public Ref<Node> {
 public:
  NPtr() : Ref<Node>(0) {}
  NPtr(Node *from) : Ref<Node>(from) {}
  inline bool isValid() const { return data != 0; }

  inline bool truth() const { return isValid(); }
  inline bool operator!() const { return !isValid(); }
};

/** Functions useful for returning child pointer lists for the
 *  cycle detection in reference_counted.
 */
extern void append_node(child_set &children, const NPtr &node);
extern void append_node_tuple(child_set &children, const NPtr &node, const NPtr &node2);
extern void append_node_tuple(child_set &children, const NPtr &node, const NPtr &node2,
                              const NPtr &node3);
extern void append_nodes(child_set &children, const vector<NPtr> &nodes);

/** Functions to conveniently build vectors of NPtrs from indiv
 *  nptrs
 */
extern vector<NPtr> nblk(const NPtr &one);
extern vector<NPtr> nblk(const NPtr &one, const NPtr &two);
extern vector<NPtr> nblk(const NPtr &one, const NPtr &two, const NPtr &three);
extern vector<NPtr> nblk(const NPtr &one, const NPtr &two, const NPtr &three, const NPtr &four);
extern vector<NPtr> nblk(const NPtr &one, const NPtr &two, const NPtr &three, const NPtr &four,
                         const NPtr &five);

/** Represents a code block
 */
class blockNode : public Node {
 public:
  vector<NPtr> statements;

  blockNode(const vector<NPtr> &stmts) : Node(), statements(stmts){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_nodes(child_list, statements); }
};

/** Represents a quoted code block object
 */
class quoteNode : public Node {
 public:
  vector<NPtr> statements;

  quoteNode(const vector<NPtr> &stmts) : Node(), statements(stmts){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_nodes(child_list, statements); }
};

/** Represents a series of statements that aren't compiled to a block
 */
class stmtListNode : public Node {
 public:
  vector<NPtr> statements;

  stmtListNode(const vector<NPtr> &stmts) : Node(), statements(stmts){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_nodes(child_list, statements); }
};

/** Represents a callable code block as value
 */
class lambdaNode : public Node {
 public:
  NPtr stmt;
  mica_string source;

  lambdaNode(const NPtr istmt, const mica_string &in_source)
      : Node(), stmt(istmt), source(in_source){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, stmt); }
};

/** Represents a callable code block as value
 */
class methodNode : public Node {
 public:
  NPtr stmt;
  mica_string source;

  methodNode(const NPtr istmt, const mica_string &isource) : Node(), stmt(istmt), source(isource){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, stmt); }
};

/** Represents an instruction to construct an argument
 */
class objectConstructorNode : public Node {
 public:
  NPtr stmt;

  objectConstructorNode(const NPtr istmt) : Node(), stmt(istmt){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, stmt); }
};

/** Represents a statement
 */
class stmtNode : public Node {
 public:
  NPtr expr;
  int line_no;

  stmtNode(const NPtr &iexpr, int line_number) : Node(), expr(iexpr), line_no(line_number){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, expr); }
};

/** Represents an expressionStatement
 */
class expressionStmtNode : public Node {
 public:
  NPtr expr;

  expressionStmtNode(const NPtr &iexpr) : Node(), expr(iexpr){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, expr); }
};

class commentNode : public Node {
 public:
  var_vector compile(Ref<Block> block, Binding &binding) const;
};

class getVerbNode : public Node {
 public:
  NPtr selector;
  NPtr arg_template;

  getVerbNode(const NPtr &in_selector, const NPtr &in_arg_template)
      : selector(in_selector), arg_template(in_arg_template){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, selector, arg_template);
  }
};

class rmVerbNode : public Node {
 public:
  NPtr selector;
  NPtr arg_template;

  rmVerbNode(const NPtr &in_selector, const NPtr &in_arg_template)
      : selector(in_selector), arg_template(in_arg_template){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, selector, arg_template);
  }
};

class setVerbNode : public Node {
 public:
  NPtr selector;
  NPtr arg_template;
  NPtr value;

  setVerbNode(const NPtr &in_selector, const NPtr &in_arg_template, const NPtr &in_value)
      : selector(in_selector), arg_template(in_arg_template), value(in_value){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, selector, arg_template, value);
  }
};

class declVerbNode : public Node {
 public:
  NPtr selector;
  NPtr arg_template;
  NPtr value;

  declVerbNode(const NPtr &in_selector, const NPtr &in_arg_template, const NPtr &in_value)
      : selector(in_selector), arg_template(in_arg_template), value(in_value){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, selector, arg_template, value);
  }
};

/** Message send
 */
class functionApplyNode : public Node {
 public:
  NPtr function;
  NPtr arguments;

  functionApplyNode(const NPtr &i_function, const NPtr &i_arguments)
      : Node(), function(i_function), arguments(i_arguments){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, function, arguments);
  }
};

/** Message send
 */
class messageNode : public Node {
 public:
  NPtr object;
  NPtr selector;
  NPtr arguments;

  messageNode(const NPtr &obj, const NPtr &sel, const NPtr &arg)
      : Node(), object(obj), selector(sel), arguments(arg){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, object, selector, arguments);
  }
};

/** Message send
 */
class qualifiedMessageNode : public messageNode {
 public:
  NPtr like;

  qualifiedMessageNode(const NPtr &obj, const NPtr &sel, const NPtr &arg, const NPtr &qualifier)
      : messageNode(obj, sel, arg), like(qualifier) {}

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, like); }
};

class passNode : public Node {
 public:
  NPtr args;
  NPtr dest;

  passNode(const NPtr &iargs, const NPtr &idest) : Node(), args(iargs), dest(idest){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node_tuple(child_list, args, dest); }
};

/** Represents an aggregation for a list.
 */
class listNode : public Node {
 public:
  vector<NPtr> values;

  listNode(const vector<NPtr> &iValues) : Node(), values(iValues){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_nodes(child_list, values); }
};

class setNode : public Node {
 public:
  vector<NPtr> values;

  setNode(const vector<NPtr> &iValues) : Node(), values(iValues){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_nodes(child_list, values); }
};

/** Represents an aggregation for a map
 */
class mapNode : public Node {
 public:
  vector<NPtr> pairs;

  mapNode(const vector<NPtr> &iPairs) : Node(), pairs(iPairs){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_nodes(child_list, pairs); }
};

class literalNode : public Node {
 public:
  Var value;

  literalNode(const Var &iValue) : Node(), value(iValue){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { child_list << value; }
};

class errorNode : public Node {
 public:
  Symbol sym;
  Ref<String> desc;

  errorNode(const Symbol &isym, Ref<String> &description) : Node(), sym(isym), desc(description){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    if ((String *)desc)
      child_list.push_back((reference_counted *)(String *)desc);
  }
};

class varDeclNode : public Node {
 public:
  Var name;
  NPtr value;

  varDeclNode(const Var &iName, const NPtr &i_value) : Node(), name(iName), value(i_value){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node(child_list, value);
    child_list << name;
  }
};

class identNode : public Node {
 public:
  Var name;

  identNode(const Var &iName) : Node(), name(iName){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { child_list << name; }
};

class assignNode : public Node {
 public:
  Var name;
  NPtr value;

  assignNode(const Var &iName, const NPtr &val) : Node(), name(iName), value(val){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node(child_list, value);
    child_list << name;
  }
};

class scatterAssignNode : public Node {
 public:
  NPtr lhs;
  var_vector required;
  var_vector optional;
  Var remainder;
  bool declare;

  scatterAssignNode(const NPtr &ilhs, const var_vector &irequired, const var_vector &ioptional,
                    const Var &iremainder, bool ideclare)
      : Node(),
        lhs(ilhs),
        required(irequired),
        optional(ioptional),
        remainder(iremainder),
        declare(ideclare){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &children) {
    append_node(children, lhs);
    append_data(children, remainder);
    var_vector::iterator x;
    for (x = required.begin(); x != required.end(); x++) append_data(children, *x);
    for (x = optional.begin(); x != optional.end(); x++) append_data(children, *x);
  }
};

class unaryNode : public Node {
 public:
  NPtr value;
  Var opcode;

  unaryNode(const NPtr &iValue, const Var &iOpcode) : Node(), value(iValue), opcode(iOpcode){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_data(child_list, opcode);
    append_node(child_list, value);
  }
};

class returnNode : public Node {
 public:
  NPtr value;

  returnNode(const NPtr &iValue) : Node(), value(iValue){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, value); }
};

class binaryNode : public Node {
 public:
  NPtr lhs;
  NPtr rhs;

  Var opcode;

  binaryNode(const NPtr &iLhs, const NPtr &iRhs, const Var &op)
      : Node(), lhs(iLhs), rhs(iRhs), opcode(op){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node_tuple(child_list, lhs, rhs); }
};

class trinaryNode : public Node {
 public:
  NPtr lhs;
  NPtr mid;
  NPtr rhs;

  Var opcode;

  trinaryNode(const NPtr &iLhs, const NPtr &iMid, const NPtr &iRhs, const Var &op)
      : Node(), lhs(iLhs), mid(iMid), rhs(iRhs), opcode(op){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, lhs, mid, rhs);
  }
};

class forNode : public Node {
 public:
  Var name;
  NPtr rangeExpr;
  NPtr branch;

  forNode(const Var &ivar, const NPtr &irangeExpr, const NPtr &iBranch)
      : Node(), name(ivar), rangeExpr(irangeExpr), branch(iBranch){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, rangeExpr, branch);
  }
};

class loopNode : public Node {
 public:
  NPtr branch;
  loopNode(const NPtr &i_branch) : branch(i_branch) {}

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, branch); }
};

class whileNode : public Node {
 public:
  NPtr testExpr;
  NPtr trueBranch;

  whileNode(const NPtr &iTestExpr, const NPtr &iTrueBranch)
      : Node(), testExpr(iTestExpr), trueBranch(iTrueBranch){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, testExpr, trueBranch);
  }
};

class doWhileNode : public Node {
 public:
  NPtr testExpr;
  NPtr trueBranch;

  doWhileNode(const NPtr &iTestExpr, const NPtr &iTrueBranch)
      : Node(), testExpr(iTestExpr), trueBranch(iTrueBranch){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, testExpr, trueBranch);
  }
};

class throwNode : public Node {
 public:
  NPtr err;

  throwNode(const NPtr &ierr) : Node(), err(ierr){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, err); }
};

class tryCatchNode : public Node {
 public:
  struct Catch {
    Var ident;
    Var err;
    NPtr branch;
  };
  vector<Catch> catchers;
  NPtr do_branch;

  tryCatchNode(const NPtr &idoBranch, const vector<Catch> &icatchers)
      : Node(), catchers(icatchers), do_branch(idoBranch) {}

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node(child_list, do_branch);
    for (vector<Catch>::iterator x = catchers.begin(); x != catchers.end(); x++) {
      append_node(child_list, x->branch);
      append_data(child_list, x->ident);
      append_data(child_list, x->err);
    }
  }
};

class ifNode : public Node {
 public:
  NPtr testExpr;
  NPtr trueBranch;

  ifNode(const NPtr &iTestExpr, const NPtr &iTrueBranch)
      : Node(), testExpr(iTestExpr), trueBranch(iTrueBranch){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) {
    append_node_tuple(child_list, testExpr, trueBranch);
  }
};

class ifElseNode : public ifNode {
 public:
  NPtr elseBranch;

  ifElseNode(const NPtr &iTestExpr, const NPtr &iTrueBranch, const NPtr &iElseBranch)
      : ifNode(iTestExpr, iTrueBranch), elseBranch(iElseBranch){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
  void append_child_pointers(child_set &child_list) { append_node(child_list, elseBranch); }
};

class noopNode : public Node {
 public:
  noopNode() : Node(){};

  var_vector compile(Ref<Block> block, Binding &binding) const;
};
}

#endif   // MICA_NODES_HH

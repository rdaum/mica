/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef NODES_HH
#define NODES_HH

#include <vector>

#include "reference_counted.hh"
#include "Data.hh"
#include "Binding.hh"
#include "Ref.hh"

using namespace std;

namespace mica { 

  class Var;
  class Block;
  class String;

  /** Parent Node class for the syntax tree.
   * 
   *  Each Node defines a "compile" interface which knows how to turn
   *  itself and its children into opcodes and values for execution
   *  by the virtual machine.
   */
  class Node
    : public reference_counted,
      public has_constant_type_identifier<Type::NODE>
  {
  private:
    friend class Ref<Node>;

  public:
    Node() : reference_counted() {};
    virtual ~Node() {};

    virtual var_vector compile( Ref<Block> block,
				Binding &binding ) const = 0;

    virtual Ref<Block> compile_to_expr( Binding &binding,
					const char *source = 0 );

    virtual child_set child_pointers();
  };

  class NPtr : public Ref<Node> 
  {
  public:
    NPtr() : Ref<Node>(0) {}
    NPtr(Node *from) : Ref<Node>(from) { }
    inline bool isValid() const { return data != 0; }

    inline bool truth() const { return isValid(); }
    inline bool operator!() const { return !isValid(); }
  };

  /** Functions useful for returning child pointer lists for the
   *  cycle detection in reference_counted.
   */
  extern void append_node( child_set &children, 
			   const NPtr &node );
  extern void append_nodes( child_set &children, 
			    const vector<NPtr> &nodes );

  extern child_set node_list( const vector<NPtr>
			      &nodes );
  

  extern child_set node_single( const NPtr &first );

  extern child_set node_pair( const NPtr &left,
			      const NPtr &right );
  
  extern child_set node_triple( const NPtr &one,
				const NPtr &two,
				const NPtr &three );
  
  /** Functions to conveniently build vectors of NPtrs from indiv
   *  nptrs
   */
  extern vector<NPtr> nblk( const NPtr &one );
  extern vector<NPtr> nblk( const NPtr &one, const NPtr &two );
  extern vector<NPtr> nblk( const NPtr &one, const NPtr &two, 
			    const NPtr &three );
  extern vector<NPtr> nblk( const NPtr &one, const NPtr &two, 
			    const NPtr &three, const NPtr &four );
  extern vector<NPtr> nblk( const NPtr &one, const NPtr &two, 
			    const NPtr &three, const NPtr &four, 
			    const NPtr &five );



  /** Represents a code block
   */
  class blockNode
    : public Node
  {
  public:
    vector<NPtr> statements;

    blockNode( const vector<NPtr> &stmts ) 
      : Node(), statements(stmts) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_list(statements);
    }

  };

  /** Represents a quoted code block object
   */
  class quoteNode
    : public Node
  {
  public:
    vector<NPtr> statements;

    quoteNode( const vector<NPtr> &stmts ) 
      : Node(), statements(stmts) {};

    var_vector compile( Ref<Block> block, Binding &binding ) const;
    child_set child_pointers() {
      return node_list(statements);
    }

  };

  /** Represents a series of statements that aren't compiled to a block
   */
  class stmtListNode
    : public Node
  {
  public:
    vector<NPtr> statements;

    stmtListNode( const vector<NPtr> &stmts ) 
      : Node(), statements(stmts) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_list(statements);
    }
  };

  /** Represents a callable code block as value
   */
  class lambdaNode
    : public Node
  {
  public:
    NPtr stmt;
    mica_string source;

    lambdaNode( const NPtr istmt, const mica_string &in_source ) 
      : Node(), stmt(istmt), source(in_source) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(stmt);
    } 
  };

  /** Represents a callable code block as value
   */
  class methodNode
    : public Node
  {
  public:
    NPtr stmt;
    mica_string source;

    methodNode( const NPtr istmt, const mica_string &isource ) 
      : Node(), stmt(istmt), source(isource) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(stmt);
    }

  };

  /** Represents an instruction to construct an argument
   */
  class objectConstructorNode
    : public Node
  {
  public:
    NPtr stmt;

    objectConstructorNode( const NPtr istmt ) 
      : Node(), stmt(istmt) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(stmt);
    } 
  };

  /** Represents a statement
   */
  class stmtNode
    : public Node
  {
  public:
    NPtr expr;
    int line_no;

    stmtNode( const NPtr &iexpr, int line_number ) 
      : Node(), expr(iexpr), line_no(line_number) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(expr);
    }
  };

  /** Represents an expressionStatement
   */
  class expressionStmtNode
    : public Node
  {
  public:
    NPtr expr;

    expressionStmtNode( const NPtr &iexpr )
      : Node(), expr(iexpr) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(expr);
    }
  };

  
  class commentNode
    : public Node
  {
  public:
    var_vector compile( Ref<Block>block, Binding &binding ) const;
  };
 
  class getVerbNode 
    : public Node {
  public:
    NPtr selector;
    NPtr arg_template;

    getVerbNode( const NPtr &in_selector, const NPtr &in_arg_template )
      : selector(in_selector), arg_template(in_arg_template) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;    
    child_set child_pointers() {
      return node_pair(selector, arg_template);
    }
  };

  class rmVerbNode 
    : public Node {
  public:
    NPtr selector;
    NPtr arg_template;

    rmVerbNode( const NPtr &in_selector, const NPtr &in_arg_template )
      : selector(in_selector), arg_template(in_arg_template) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;    
    child_set child_pointers() {
      return node_pair(selector, arg_template);
    }
  };

  class setVerbNode 
    : public Node {
  public:
    NPtr selector;
    NPtr arg_template;
    NPtr value;

    setVerbNode( const NPtr &in_selector, const NPtr &in_arg_template, 
		 const NPtr &in_value )
      : selector(in_selector), arg_template(in_arg_template),
	value(in_value) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;    
    child_set child_pointers() {
      return node_triple(selector, arg_template, value);
    }
  };

  class declVerbNode 
    : public Node {
  public:
    NPtr selector;
    NPtr arg_template;
    NPtr value;

    declVerbNode( const NPtr &in_selector, const NPtr &in_arg_template, 
		  const NPtr &in_value )
      : selector(in_selector), arg_template(in_arg_template),
	value(in_value) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;    
    child_set child_pointers() {
      return node_triple(selector, arg_template, value);
    }
  };

  /** Message send
   */
  class functionApplyNode
    : public Node
  {
  public:
    NPtr function;
    NPtr arguments;

    functionApplyNode( const NPtr &i_function, const NPtr &i_arguments)
      : Node(), function(i_function), arguments(i_arguments)
    {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair(function, arguments);
    }
  };


  /** Message send
   */
  class messageNode
    : public Node
  {
  public:
    NPtr object;
    NPtr selector;
    NPtr arguments;

    messageNode( const NPtr &obj, const NPtr &sel, const NPtr &arg )
      : Node(), object(obj),
	selector(sel),
	arguments(arg) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_triple(object, selector, arguments);
    }
  };

  /** Message send
   */
  class qualifiedMessageNode
    : public messageNode
  {
  public:
    NPtr like;

    qualifiedMessageNode( const NPtr &obj, const NPtr &sel, const NPtr &arg,
			  const NPtr &qualifier )
      : messageNode( obj, sel, arg ), like(qualifier) {}

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      child_set children(this->messageNode::child_pointers());
      append_node( children, like );
      return children;
    }
  };

  class passNode
    : public Node
  {
  public:
    NPtr args;
    NPtr dest;

    passNode( const NPtr &iargs, const NPtr &idest )
      : Node(), args(iargs), dest(idest) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair( args, dest );
    }
  };

  /** Represents an aggregation for a list. 
   */
  class listNode
    : public Node
  {
  public:
    vector<NPtr> values;

    listNode( const vector<NPtr> &iValues )
      : Node(), values(iValues) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_list(values);
    }
  };


  class setNode
    : public Node
  {
  public:
    vector<NPtr> values;

    setNode( const vector<NPtr> &iValues )
      : Node(), values(iValues) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_list(values);
    }
  };

  /** Represents an aggregation for a map
   */
  class mapNode
    : public Node
  {
  public:
    vector<NPtr> pairs;

    mapNode( const vector<NPtr> &iPairs )
      : Node(), pairs(iPairs) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_list(pairs);
    }
  };

  class literalNode
    : public Node
  {
  public:
    Var value;

    literalNode( const Var &iValue )
      : Node(), value(iValue) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() { 
      child_set children;
      append_data( children, value );
      return children;
    }
  };


  class errorNode
    : public Node
  {
  public:
    Symbol sym;
    Ref<String> desc;

    errorNode( const Symbol &isym, Ref<String> &description )
      : Node(), sym(isym), desc(description) {};
   
    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() { 
      child_set children;
      if ( (String*)desc )
	children.push_back( (reference_counted*)(String*)desc );
      return children;
    }
  };


  class varDeclNode
    : public Node
  {
  public:
    Var name;
    NPtr value;

    varDeclNode( const Var &iName, const NPtr &i_value )
      : Node(), name(iName), value(i_value)  {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() { 
      child_set children;
      append_data( children, name );
      append_node( children, value );
      return children;
    }
  };

  class identNode
    : public Node
  {
  public:
    Var name;

    identNode( const Var &iName )
      : Node(), name(iName) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() { 
      child_set children;
      append_data( children, name );
      return children;
    }
  };

  class assignNode
    : public Node
  {
  public:
    Var name;
    NPtr value;

    assignNode( const Var &iName, const NPtr &val  )
      : Node(), name(iName), value(val) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      child_set children(node_single(value));
      children << name;
      return children;
    }
  };


  class scatterAssignNode
    : public Node
  {
  public:
    NPtr lhs;
    var_vector required;
    var_vector optional;
    Var remainder;
    bool declare;

    scatterAssignNode( const NPtr &ilhs, 
		       const var_vector &irequired,
		       const var_vector &ioptional,
		       const Var &iremainder, 
		       bool ideclare )
      : Node(), lhs(ilhs), required(irequired), optional(ioptional),
	remainder(iremainder), declare(ideclare) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      child_set children;
      append_node( children, lhs );
      append_data( children, remainder );
      var_vector::iterator x;
      for (x = required.begin(); x != required.end(); x++) 
	append_data( children, *x );
      for (x = optional.begin(); x != optional.end(); x++) 
	append_data( children, *x );
      return children;
    }

  };



  class unaryNode
    : public Node
  {
  public:
    NPtr value;
    Var opcode;

    unaryNode( const NPtr &iValue, const Var &iOpcode )
      : Node(), value(iValue),
	opcode(iOpcode) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() { 
      child_set children;
      append_data( children, opcode );
      append_node( children, value );
      return children;
    }

  };


  class returnNode
    : public Node
  {
  public:
    NPtr value;

    returnNode( const NPtr &iValue )
      : Node(), value(iValue) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(value);
    }
  };

  class binaryNode
    : public Node
  {
  public:
    NPtr lhs;
    NPtr rhs;

    Var opcode;

    binaryNode( const NPtr &iLhs, const NPtr &iRhs, const Var &op  )
      : Node(), lhs(iLhs), rhs(iRhs), opcode(op) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair(lhs, rhs);
    }
  };

  class trinaryNode
    : public Node
  {
  public:
    NPtr lhs;
    NPtr mid;
    NPtr rhs;

    Var opcode;

    trinaryNode( const NPtr &iLhs, const NPtr &iMid,
		const NPtr &iRhs, const Var &op  )
      : Node(), lhs(iLhs), mid(iMid), rhs(iRhs), opcode(op) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_triple(lhs, mid, rhs);
    }
  };

  class forNode
    : public Node 
  {
  public:
    Var  name;
    NPtr rangeExpr;
    NPtr branch;
    
    forNode( const Var &ivar, const NPtr &irangeExpr, 
	     const NPtr &iBranch)
      : Node(), name(ivar), rangeExpr(irangeExpr),
	branch(iBranch) {};
  
    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair( rangeExpr, branch );
    }
  };


  class loopNode
    : public Node
  {
  public:
    NPtr branch;
    loopNode( const NPtr &i_branch )
      : branch(i_branch) {}

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single( branch );
    }
  };

  class whileNode
    : public Node
  {
  public:
    NPtr testExpr;
    NPtr trueBranch;

    whileNode( const NPtr &iTestExpr, const NPtr &iTrueBranch )
      : Node(), testExpr( iTestExpr ),
	trueBranch( iTrueBranch ) {};
  
    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair( testExpr, trueBranch );
    }
  };

  class doWhileNode
    : public Node
  {
  public:
    NPtr testExpr;
    NPtr trueBranch;

    doWhileNode( const NPtr &iTestExpr, const NPtr &iTrueBranch )
      : Node(), testExpr( iTestExpr ),
	trueBranch( iTrueBranch ) {};
  
    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair( testExpr, trueBranch );
    }
  };


  class throwNode
    : public Node
  {
  public:
    NPtr err;

    throwNode( const NPtr &ierr)
      : Node(), err(ierr) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(err);
    }
  };

  class tryCatchNode 
    : public Node
  {
  public:
    struct Catch {
      Var ident;
      Var err;
      NPtr branch;
    };
    vector<Catch> catchers;
    NPtr do_branch;

    tryCatchNode( const NPtr &idoBranch, const vector<Catch> &icatchers )
      : Node(), catchers(icatchers), do_branch(idoBranch) {}

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      child_set children;
      append_node( children, do_branch );
      for (vector<Catch>::iterator x = catchers.begin();
	   x != catchers.end(); x++) {
	append_node( children, x->branch );
	append_data( children, x->ident );
	append_data( children, x->err );
      }
      return children;
    }
  };


  class ifNode
    : public Node
  {
  public:
    NPtr testExpr;
    NPtr trueBranch;

    ifNode( const NPtr &iTestExpr, const NPtr &iTrueBranch )
      : Node(), testExpr( iTestExpr ),
	trueBranch( iTrueBranch ) {};
  
    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_pair(testExpr, trueBranch);
    }
  };

  class ifElseNode
    : public ifNode
  {
  public:
    NPtr elseBranch;

    ifElseNode( const NPtr &iTestExpr, const NPtr &iTrueBranch,  
		const NPtr &iElseBranch )
      : ifNode(iTestExpr, iTrueBranch),
	elseBranch(iElseBranch) {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
    child_set child_pointers() {
      return node_single(elseBranch);
    }
  };


  class noopNode
    : public Node
  {
  public:
    noopNode() : Node() {};

    var_vector compile( Ref<Block>block, Binding &binding ) const;
  };

}

#endif /* NODES_HH */

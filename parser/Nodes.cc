#include <iostream>

#include "Data.hh"
#include "Var.hh"
#include "Error.hh"
#include "Exceptions.hh"
#include "Task.hh"
#include "Object.hh"
#include "List.hh"
#include "Scalar.hh"
#include "Symbol.hh"

#include "Block.hh"

#include "Nodes.hh"


using namespace std;
using namespace mica;

void mica::append_node( child_set &children, 
		    const NPtr &node ) {
  children.push_back( ((Node*)node) );
}

void mica::append_nodes( child_set &children,
		   const vector<NPtr> &nodes ) {
  for (vector<NPtr>::const_iterator x = nodes.begin();
	 x != nodes.end(); x++ ) {
    children.push_back( ((Node*)*x) );
  }
}

child_set mica::node_list( const vector<NPtr> &nodes ) {
  child_set children;
  append_nodes( children, nodes );
  return children;
}

child_set mica::node_single( const NPtr &one ) {
  child_set children;
  append_node( children, one );
  return children;
}

child_set mica::node_pair( const NPtr &left,
				    const NPtr &right ) {
  child_set children;
  append_node( children, left );
  append_node( children, right );

  return children;
}

child_set mica::node_triple( const NPtr &one,
					   const NPtr &two,
					   const NPtr &three ) {
  child_set children;
  append_node( children, one );
  append_node( children, two );
  append_node( children, three );

  return children;
}

// EMPTY NODE
var_vector Node::compile( Ref<Block> block, Binding &binding ) const {

  // Just return a blank vector
  var_vector ops;
  return ops;
}

child_set Node::child_pointers() {
  return child_set();
}

// COMMENT NODE
var_vector commentNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  return ops;
}

      
var_vector blockNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  
  ops.push_back( Var(Op::BBEGIN ));

  binding.startBlock();

  /** Compile all statments
   */
  var_vector n_ops;
  for (vector<NPtr>::const_iterator i = statements.begin();
       i != statements.end(); i++) {
    NPtr stmt = *i;
    var_vector append = stmt->compile( block, binding );
    
    n_ops.insert( n_ops.end(), append.begin(), append.end() );

  }

  /** Append the # of local vars for the block
   */
  ops.push_back( Var( (int)binding.finishBlock()) );

  unsigned int size_pos = ops.size() ;
  ops.push_back( Var(0) );

  /** Append the statements
   */
  ops.insert( ops.end(), n_ops.begin(), n_ops.end() );

  /** Finish the block
   */
  ops.push_back( Var(Op::BEND ));

  ops[size_pos] = Var( (int)ops.size() );

  return ops;
}


var_vector quoteNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  Ref<Block> q_block = new (aligned) Block("");

  /** Compile all statments
   */
  var_vector n_ops;
  for (vector<NPtr>::const_iterator i = statements.begin();
       i != statements.end(); i++) {
    NPtr stmt = *i;
    var_vector append = stmt->compile( block, binding );
    
    n_ops.insert( n_ops.end(), append.begin(), append.end() );

  }
  q_block->code = n_ops;

  ops.push_back( Var(q_block) );

  return ops;
}

var_vector stmtListNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  
  /** Compile and append all values
   */

  for (vector<NPtr>::const_iterator i = statements.begin();
       i != statements.end(); i++) {
    NPtr stmt = *i;
    var_vector append = stmt->compile( block, binding );
    
    ops.insert( ops.end(), append.begin(), append.end() );

  }

  return ops;
}

var_vector lambdaNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Create a new (aligned) block, just for counting line #s, etc.
   */
  Ref<Block> lambda = new (aligned) Block(source);

  /** Start a new (aligned) block in the binding.
   */
  binding.startBlock();

  /** Compile the operations with new (aligned) block as guidance
   */
  var_vector code_ops = stmt->compile( lambda, binding );

  /** Grab how many additional vars there are
   */
  unsigned int add_vars = binding.finishBlock();
  lambda->add_scope = add_vars;
  lambda->code = code_ops;

  /** Push the lambda code into the code section of current block
   */
  var_vector ops;
  ops.push_back( Var(lambda) );

  /** Push MAKE_LAMBDA
   */
  ops.push_back( Var(Op::MAKE_LAMBDA) );


  return ops;
}


var_vector objectConstructorNode::compile( Ref<Block> block, 
					   Binding &binding ) const {

  /** Create a new (aligned) block containing the instructions
   */
  Ref<Block> object_block = new (aligned) Block("");

  /** Start a new (aligned) block in the binding.
   */
  binding.startBlock();

  /** Declare the `creator' variable
   */
  unsigned int c_pos = 0;
  binding.define( Var(Symbol::create("creator")) );
  
  /** Compile the operations with new (aligned) block as guidance
   */
  object_block->code = stmt->compile( object_block, binding );

  object_block->add_scope = binding.finishBlock();

  /** Push a "return self" at the end of the block just in case that block
   *  never returns anything.
   */
  object_block->code.push_back( Var(Op::SELF ));
  object_block->code.push_back( Var(Op::RETURN ));

  /** Push the object construction block into the stack
   */
  var_vector ops;
  ops.push_back( Var(object_block) );

  /** Push MAKE_OBJECT followed by the var# of the "creator" var.
   */
  ops.push_back( Var( Op( Op::MAKE_OBJECT, c_pos )));
  
  ops.push_back( List::empty() );

  ops.push_back( Var(Op::PERFORM) );

  return ops;
}


var_vector methodNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Create a new (aligned) block
   */
  Ref<Block> new_block(new (aligned) Block(source));

  /** Start a new (aligned) binding here, and compile into the new (aligned) block
   */
  Binding new_binding;
  binding.startBlock();
  var_vector code_ops = stmt->compile( new_block, new_binding) ;
  binding.finishBlock();

  /** Set the block's code to compile of the statements.
   */
  new_block->code = code_ops;

  /** Push the block object itself into the code section of current block
   */
  var_vector ops;
  ops.push_back( Var(new_block) );

  return ops;
}

var_vector expressionStmtNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops = expr->compile( block, binding);

  /** Don't pop anything if the expression didn't _do_ anything.
   */
  if (ops.size())
    ops.push_back( Var(Op::POP ));

  return ops;
}

var_vector stmtNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile the expression
   */
  var_vector ops = expr->compile( block, binding);
  
  /** Tell the block about the length of the ops --
   *  this is the relative position from the last end of
   *  line.  At traceback, current line is computed by
   *  adding up each relative position until the PC
   *  value is obtained.  The number of additions =
   *  the line # where the suspension occured.
   */
  block->add_line( ops.size(), line_no );

  return ops;
}

var_vector getVerbNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile the selector
   */
  var_vector ops = selector->compile( block, binding ) ;

  /** Append the argument template
   */
  var_vector arg_ops = arg_template->compile( block, binding );
  ops.insert( ops.end(), arg_ops.begin(), arg_ops.end() );

  /** get the slot.  it should end up on the stack
   */
  ops.push_back( Var(Op::GETVERB ));

  return ops;
}

var_vector rmVerbNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile the selector
   */
  var_vector ops = selector->compile( block, binding ) ;

  /** Append the argument template
   */
  var_vector arg_ops = arg_template->compile( block, binding );
  ops.insert( ops.end(), arg_ops.begin(), arg_ops.end() );

  /** remove the slot.  it should end up on the stack
   */
  ops.push_back( Var(Op::RMVERB ));

  return ops;
}

var_vector setVerbNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile the value
   */
  var_vector ops = value->compile( block, binding ) ;

  /** Append the argument template
   */
  var_vector arg_ops = arg_template->compile( block, binding );
  ops.insert( ops.end(), arg_ops.begin(), arg_ops.end() );

  /** Append the selector
   */
  var_vector sel_ops = selector->compile( block, binding );

  ops.insert( ops.end(), sel_ops.begin(), sel_ops.end() );

  /** set the slot
   */
  ops.push_back( Var(Op::SETVERB ));

  return ops;
}

var_vector declVerbNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile the value
   */
  var_vector ops = value->compile( block, binding ) ;

  /** Append the argument template
   */
  var_vector arg_ops = arg_template->compile( block, binding );
  ops.insert( ops.end(), arg_ops.begin(), arg_ops.end() );

  /** Append the selector
   */
  var_vector sel_ops = selector->compile( block, binding );

  ops.insert( ops.end(), sel_ops.begin(), sel_ops.end() );

  /** decl the slot
   */
  ops.push_back( Var(Op::DECLVERB ));

  return ops;
}

var_vector messageNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  if (arguments)
    ops = arguments->compile( block, binding );
  var_vector sel = selector->compile( block, binding );
  var_vector args = object->compile( block, binding );

  ops.insert( ops.end(), sel.begin(), sel.end() );
  ops.insert( ops.end(), args.begin(), args.end() );

  /** SEND creates message
   */
  ops.push_back( Var(Op::SEND ));

  /** PERFORM on the msg actually does the message dispatch.
   */
  ops.push_back( Var(Op::PERFORM ));

  return ops;
}

var_vector
qualifiedMessageNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops(like->compile( block, binding ) );
  var_vector args;
  if (arguments)
    args = arguments->compile( block, binding );
  var_vector sel = selector->compile( block, binding );
  var_vector dest = object->compile( block, binding );

  ops.insert( ops.end(), args.begin(), args.end() );
  ops.insert( ops.end(), sel.begin(), sel.end() );
  ops.insert( ops.end(), dest.begin(), dest.end() );

  /** SEND_LIKE creates a qualified message
   */
  ops.push_back( Var(Op::SEND_LIKE ));

  /** PERFORM on the msg actually does the message dispatch.
   */
  ops.push_back( Var(Op::PERFORM ));

  return ops;
}

var_vector passNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  if (args)
    ops = args->compile( block, binding );
  else
    ops.push_back( Var() );

  if (dest) {
    var_vector dest_o = dest->compile( block, binding );
    ops.insert( ops.end(), dest_o.begin(), dest_o.end() );
  } else {
    ops.push_back( Var() );
  }
  ops.push_back( Var(Op::PASS ));

  return ops;
}

var_vector listNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  ops.push_back( Var(Op::LIST_MARKER ));

  /** Compile and append all values
   */
  for (vector<NPtr>::const_reverse_iterator i = values.rbegin(); i != values.rend(); i++) {
    var_vector append = (*i)->compile( block, binding );

    ops.insert( ops.end(), append.begin(), append.end());
  }
    

  /** Push POP_LIST
   */
  ops.push_back( Var(Op::POP_LIST ));

  
  return ops;
}



var_vector setNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  ops.push_back( Var(Op::SET_MARKER ));

  /** Compile and append all values
   */
  for (vector<NPtr>::const_reverse_iterator i = values.rbegin(); i != values.rend(); i++) {
    var_vector append = (*i)->compile( block, binding );

    ops.insert( ops.end(), append.begin(), append.end());
  }
    
  /** Push POP_SET
   */
  ops.push_back( Var(Op::POP_SET ));
 
  return ops;
}

var_vector mapNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  ops.push_back( Var(Op::MAP_MARKER ));

  /** Compile and append all values
   */
  for (vector<NPtr>::const_iterator i = pairs.begin(); i != pairs.end(); i++) {
    var_vector append = (*i)->compile( block, binding );

    ops.insert( ops.end(), append.begin(), append.end() );
  }
    
  /** Push POP_LIST
   */
  ops.push_back( Var(Op::POP_MAP ));
  
  return ops;
}


var_vector literalNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Just push the literal to a vector and return it
   */
  var_vector ops;

  ops.push_back( value );

  return ops;
}



var_vector errorNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Names are symbols which are then looked up at runtime
   *  from the global dictionary and an object returned.
   */
  var_vector ops;

  Var err = new (aligned) Error( sym, desc );

  ops.push_back( err );

  return ops;
}

var_vector assignNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile the values
   */
  var_vector ops = value->compile( block, binding);

  /** Look the name up in the block
   */
  int id = binding.lookup( name );


  /** Now push a SETVAR for it
   */
  ops.push_back( Var( Op( Op::SETVAR, id ) ) );

  return ops;
}


var_vector scatterAssignNode::compile( Ref<Block> block, 
				       Binding &binding ) const {
  // do we have to declare everything?  If so, do so now.

  var_vector::const_iterator x;
  bool has_remainder = (remainder != Var(false));
  if (declare) {

    for (x = required.begin();
	 x != required.end(); x++) {
      // Declare the name in the binding.
      binding.define( *x );
    }

    for (x = optional.begin();
	 x != optional.end(); x++) {
      // Declare the name in the binding.
      binding.define( *x );
    }
    if (has_remainder)
      binding.define(remainder);
  }
  
  /** First we compile the range!
   */
  var_vector ops = lhs->compile( block, binding);
 
  /** Push the SCATTER ops
   */
  ops.push_back( Var(Op::SCATTER ));

  /** Push the # of required vars
   */
  ops.push_back( Var( (int)required.size()) );

  /** Now push each of their ids
   */
  for (x = required.begin(); x != required.end();
       x++)
    ops.push_back( Var((int)binding.lookup( *x )) );
  
  /** Push the # of optional vars
   */
  ops.push_back( Var( (int)optional.size()) );

  /** Now push each of their ids
   */
  for (x = optional.begin(); x != optional.end();
       x++) 
    ops.push_back( Var( (int)binding.lookup( *x )) );
  
  /** If there's a remainder, let the vm know...
   */
  ops.push_back( Var(has_remainder) );

  if (has_remainder) {
    // Now the remainder
    ops.push_back( Var( (int)binding.lookup( remainder )) );
  }

  return ops;
}

var_vector varDeclNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Declare the name in the binding.
   */
  binding.define( name );
  int id = binding.lookup( name );

  var_vector ops;
  /** If there is a value, then we insert the assignment for it
   */
  if (value.isValid()) {
    ops = value->compile( block, binding );

    ops.push_back( Var( Op( Op::SETVAR, id ) ) );

    /** can't leave anything on stack
     */
    ops.push_back( Var(Op::POP ));

  }

  return ops;
}

var_vector identNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  /** Look the name up in the block
   */
  size_t id = binding.lookup( name );

  /** Now push a GETVAR for it
   */
  ops.push_back( Var( Op( Op::GETVAR, id) ) );

  return ops;
}

var_vector unaryNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile value
   */
  var_vector ops = value->compile( block, binding );

  /** Push the operation
   */
  ops.push_back( opcode );

  return ops;
}

var_vector returnNode::compile( Ref<Block> block, Binding &binding ) const {

  var_vector ops;
  if (value) {
    /** Compile return value
     */
    ops = value->compile( block, binding );
  } else {
    ops.push_back( NONE );
  }

  /** Push the operation
   */
  ops.push_back( Var(Op::RETURN ));

  return ops;
}


var_vector binaryNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile left hand side
   */
  var_vector ops = lhs->compile( block, binding );

  /** Compile and append right hand side
   */
  var_vector rhc = rhs->compile( block, binding );
  
  ops.insert( ops.end(), rhc.begin(), rhc.end() );
  
  /** Push the operation
   */
  ops.push_back( opcode );

  return ops;
}

var_vector trinaryNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Compile left hand side
   */
  var_vector ops = lhs->compile( block, binding );

  /** Compile and append middle
   */
  var_vector mc = mid->compile( block, binding );
  ops.insert( ops.end(), mc.begin(), mc.end() );

  /** Compile and append right hand side
   */
  var_vector rhc = rhs->compile( block, binding );
  ops.insert( ops.end(), rhc.begin(), rhc.end() );  

  
  /** Push the operation
   */
  ops.push_back( opcode );

  return ops;
}


var_vector forNode::compile( Ref<Block> block, Binding &binding ) const {

  /** compile the sequence expr (pushes the IN value to stack
   */
  var_vector ops( rangeExpr->compile( block, binding ) );

  /** compile and append the block to execute
   */
  var_vector block_ops( branch->compile( block, binding ) );
  Ref<Block> our_block = new (aligned) Block("");
  our_block->code = block_ops;
  ops.push_back( Var(our_block) );

  

  /** Cast the name from Node to identNode, grab the name symbol
   *  from it, and use its value.  NASTY UGLY CAST.
   */
  identNode *ident = ((identNode*)(Node*)name);
  Var name_val = ident->name;

  /** Now push the FOR_RANGE
   */
  ops.push_back( Var( Op( Op::FOR_RANGE, binding.lookup( name_val ) ) ) );


  return ops;
}

var_vector whileNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Compile the branch
   */
  var_vector tb_ops = trueBranch->compile( block, binding );

  /** Compile the truth test
   */
  var_vector while_chunk( testExpr->compile(block,binding ) );

  /** Add the while, and then the branch
   */
  while_chunk.push_back( Var(Op::WHILE ));
  while_chunk.insert( while_chunk.end(), tb_ops.begin(), tb_ops.end() );
  while_chunk.push_back( Var(Op::CONTINUE) );

  var_vector ops;
  ops.push_back( Var( Op( Op::START_LOOP, while_chunk.size() + 1 ) ) );

  /** Push the operations
   */
  ops.insert( ops.end(), while_chunk.begin(), while_chunk.end() );

  return ops;
}


var_vector doWhileNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Compile the branch
   */
  var_vector tb_ops = trueBranch->compile( block, binding );

  /** Compile truth test as an if else with continue + break
   */

  NPtr if_else = new (aligned) ifNode( new (aligned) unaryNode(testExpr, Var(Op::NOT)),
				       new (aligned) literalNode( Var(Op::BREAK ) ));

  var_vector if_else_ops( if_else->compile( block, binding ) );
  if_else_ops.push_back( Var(Op::CONTINUE ));

  /** The whole thing as a chunk
   */
  var_vector chunk(tb_ops);
  chunk.insert( chunk.end(), if_else_ops.begin(), if_else_ops.end() );

  var_vector ops;
  ops.push_back( Var( Op( Op::START_LOOP, chunk.size() + 1 ) ) );

  /** Push the operations
   */
  ops.insert( ops.end(), chunk.begin(), chunk.end() );

  return ops;
}

var_vector throwNode::compile( Ref<Block> block, Binding &binding ) const {
  /** Push the error
   */
  var_vector ops = err->compile( block, binding );

  /** Push the throw op
   */
  ops.push_back(Var(Op::THROW));

  return ops;
}


var_vector tryCatchNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  /** Wrap it up in a binding.
   */
  ops.push_back( Var(Op::BBEGIN ));
  ops.push_back( Var(0) );

  unsigned int size_pos = ops.size();
  ops.push_back( Var(0) );

  /** Compile the if-it-doesn't-throw-ops
   */
  var_vector pass_ops = do_branch->compile( block, binding );
    
  /** Compile what will fail
   */
  unsigned int total_size = pass_ops.size();
  vector< var_vector > catch_blocks;
  for (vector<Catch>::const_iterator x = catchers.begin();
       x != catchers.end(); x++) {
    var_vector catcher_ops = x->branch->compile( block, binding );
    catcher_ops.push_back( Var(Op::JMP ));
    catcher_ops.push_back( Var(0) );

    var_vector catcher;
    catcher.push_back( x->err );
    catcher.push_back( Var(Op::CATCH ));
    catcher.push_back( Var( (int)binding.lookup( x->ident )) );
    catcher.push_back( Var( (int)catcher_ops.size() ) );
    catcher.insert( catcher.end(), catcher_ops.begin(), catcher_ops.end() );
    
    catch_blocks.push_back(catcher);
    total_size += catcher.size();
  }

  unsigned int jmp_count = total_size;
  for (vector<var_vector>::iterator x = catch_blocks.begin();
       x != catch_blocks.end(); x++) {
    jmp_count -= x->size();
    
    (x->begin() + x->size() - 1)->operator=((int)jmp_count);

    ops.insert( ops.end(), x->begin(), x->end() );
  }

  ops.insert( ops.end(), pass_ops.begin(), pass_ops.end() );

  ops.push_back( Var(Op::BEND ));

  ops[size_pos] = Var( (int)ops.size() );

  return ops;
}

var_vector ifNode::compile( Ref<Block> block, Binding &binding ) const {
  // Program: truth-expr exec-success IF

  /** Push the truth test first
   */
  var_vector ops = testExpr->compile( block, binding );
    
  /** Push the the success branch
   */
  var_vector tb_ops = trueBranch->compile( block, binding );
  ops.push_back( List::from_vector( tb_ops ) );

  /** Push the fail branch (empty)
   */
  ops.push_back( List::empty() );

  /** Push IFELSE opcode
   */
  ops.push_back( Var(Op::IFELSE ));

  return ops;
}
    
var_vector ifElseNode::compile( Ref<Block> block, Binding &binding ) const {
  // Program: truth-expr exec-fail exec-success IF

  /** Push the truth test first
   */
  var_vector ops = testExpr->compile( block, binding );
    
  /** Push the success branch
   */
  var_vector tb_ops = trueBranch->compile( block, binding );
  ops.push_back( List::from_vector( tb_ops ) );
  
  /** Push the fail branch
   */
  var_vector else_ops = elseBranch->compile( block, binding );
  ops.push_back( List::from_vector( else_ops ) );

  /** Push IFELSE opcode
   */
  ops.push_back( Var(Op::IFELSE ));

  return ops;
}





var_vector noopNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector a;
  return a;
}

#include <iostream>

#include "Data.hh"
#include "Var.hh"
#include "Error.hh"
#include "Exceptions.hh"
#include "Task.hh"
#include "Object.hh"
#include "List.hh"
#include "Atom.hh"
#include "Symbol.hh"

#include "Block.hh"

#include "Nodes.hh"


using namespace std;
using namespace mica;

vector<NPtr> mica::nblk( const NPtr &one ) {
  vector<NPtr> x;
  x.push_back( one );
  return x;
}

vector<NPtr> mica::nblk( const NPtr &one, const NPtr &two )  {
  vector<NPtr> x;
  x.push_back( one );
  x.push_back( two );
  return x;
}

vector<NPtr> mica::nblk( const NPtr &one, const NPtr &two,
			 const NPtr &three )  {
  vector<NPtr> x;
  x.push_back( one );
  x.push_back( two );
  x.push_back( three );
  return x;
}

vector<NPtr> mica::nblk( const NPtr &one, const NPtr &two, 
			 const NPtr &three, const NPtr &four )  {
  vector<NPtr> x;
  x.push_back( one );
  x.push_back( two );
  x.push_back( three );
  x.push_back( four );
  return x;
}

vector<NPtr> mica::nblk( const NPtr &one, const NPtr &two, 
			 const NPtr &three, const NPtr &four, 
			 const NPtr &five )  {
  vector<NPtr> x;
  x.push_back( one );
  x.push_back( two );
  x.push_back( three );
  x.push_back( four );
  x.push_back( five );
  return x;
}


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

Ref<Block> Node::compile_to_expr( Binding &binding, 
				  const mica_string &source_str ) {

  Ref<Block> expr = new Block( source_str );
  binding.startBlock();

  var_vector ops = compile( expr, binding );

  expr->code = ops;
  expr->add_scope = binding.finishBlock();

  return expr;
  
}


// COMMENT NODE
var_vector commentNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  return ops;
}

      
var_vector blockNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  
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

  Ref<Block> block_o( new Block("") );
  block_o->code = n_ops;
  block_o->add_scope = binding.finishBlock();

  ops.push_back( Var(block_o) );
  ops.push_back( Var(Op::EVAL) );

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
  ops.push_back( Var(Op::EVAL) );

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
  var_vector ops;

  /** Create the block object.
   */
  Ref<Block> lambda = stmt->compile_to_expr( binding, source );
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
  unsigned int c_ps = binding.define( Var(Symbol::create("creator")) );
  
  /** Compile the operations with new block as guidance
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
  ops.push_back( Var( Op( Op::MAKE_OBJECT, c_ps ) ) );
  
  ops.push_back( List::empty() );

  ops.push_back( Var(Op::PERFORM) );

  return ops;
}


var_vector methodNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  Ref<Block> method_block = stmt->compile_to_expr( binding, source );
  ops.push_back( Var(method_block) );

  return ops;
}

var_vector expressionStmtNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops = expr->compile( block, binding);

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

var_vector functionApplyNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;
  ops = function->compile( block, binding );

  var_vector arg_ops = arguments->compile( block, binding );

  ops.insert( ops.end(), arg_ops.begin(), arg_ops.end() );

  ops.push_back( Var(Op::PERFORM ));

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
  unsigned int id = binding.lookup( name );


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

  unsigned int opt_plus_remain = (optional.size() << 1);
  if (has_remainder) 
    opt_plus_remain |= 0x01;

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
    ops.push_back( Var(Op(Op::SCATTER, required.size(), opt_plus_remain )));

    /** Push each required var id
     */
    for (x = required.begin(); x != required.end();
         x++)
      ops.push_back( Var((int)binding.lookup( *x )) );
  
    /** Now push each of the optional ids
     */
    for (x = optional.begin(); x != optional.end();
         x++) 
      ops.push_back( Var( (int)binding.lookup( *x )) );
  
    if (has_remainder) {
      // Now push the remainder id
      ops.push_back( Var( (int)binding.lookup( remainder )) );
    }

  return ops;
}

var_vector varDeclNode::compile( Ref<Block> block, Binding &binding ) const {

  /** Declare the name in the binding.
   */
  unsigned int id = binding.define( name );

  var_vector ops;
  /** If there is a value, then we insert the assignment for it
   */
  if (value.isValid()) {
    ops = value->compile( block, binding );

    ops.push_back( Var( Op( Op::SETVAR, id ) ) );

  }

  return ops;
}

var_vector identNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector ops;

  /** Look the name up in the block
   */
  unsigned int id = binding.lookup( name );

  /** Now push a GETVAR for it
   */
  ops.push_back( Var( Op( Op::GETVAR, id ) ) );

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

  /** Rewritten to:

     new for_loop_var := rangeExpr;
     loop {
       if (for_loop_var) {
         name := car for_loop_var;
         branch();
	 for_loop_var = cdr for_loop_var;
         if (for_loop_var)
            continue;
	 else
            break;
       } else {
         break;
       }
     }

  **/
 
         
  Var vname = Var(Symbol::create( "for_loop_var"));

  NPtr on_range = new 
    blockNode(nblk( 
		   
		   /** name = car vname
		    */
		   new                      
		   assignNode( name,
			       new 
			       unaryNode( new identNode( vname ), Var(Op::CAR) ) ),
		   // vname = cdr vname; 

		   new assignNode( vname, new 
				   unaryNode( new 
					      identNode( vname ), Var(Op::CDR) ) ), 
		   /** branch()
		    */
		   branch, 
  
		   /** if (vname) continue; else break; **/
		   new
		   ifElseNode( new
			       identNode( vname ),
			       
			       new
			       blockNode( nblk( new 
						literalNode( Var(Op::CONTINUE) ) ) ),
			       
			       new 
			       blockNode( nblk( new
						literalNode( Var(Op::BREAK) ) ) ) 
			       ) 
		   ) 
	      );
  
  NPtr on_done = new 
    blockNode( nblk( new
		     literalNode( Var(Op::BREAK) ) ) );

  NPtr select_node = new 
    ifElseNode( new 
		identNode( vname ),
		on_range, on_done );

  NPtr loop_node = new 
    loopNode( new
	      blockNode( nblk( select_node ) ) );
  
  NPtr forEach = new 
    blockNode( nblk( new 
		     varDeclNode( vname, rangeExpr ),
		     loop_node ) );
					   
  return forEach->compile( block, binding );
}

var_vector loopNode::compile( Ref<Block> block, Binding &binding ) const {

		 
  /** Loop on block
   */
  var_vector ops;
  ops.push_back( Var(branch->compile_to_expr( binding ) ) );
  ops.push_back( Var(Op::LOOP) );


  return ops;
}

var_vector whileNode::compile( Ref<Block> block, Binding &binding ) const {

  NPtr while_node = new 
    loopNode( new
	      blockNode( nblk( new
			       ifElseNode( testExpr, 
					   new 
					   blockNode( nblk( trueBranch,
							    new literalNode( Var(Op::CONTINUE) ) ) ),
					   new
					   blockNode( nblk( new literalNode( Var(Op::BREAK) ) ) ) ) ) ) );
  
  return while_node->compile( block, binding );

//   /** Build the if-else block
//    */
//   var_vector select( testExpr->compile( block, binding ) );
//   Ref<Block> succ = trueBranch->compile_to_expr( binding );
//   succ->code.push_back( Var(Op::CONTINUE) );  
//   select.push_back( Var(succ) );
//   select.push_back( Var(Op::IF) );
//   select.push_back( Var(Op::BREAK ) );
 
//   /** Put it in a block
//    */
//   Ref<Block> loop_expr = new Block("");
//   loop_expr->code = select;
//   loop_expr->add_scope = 0;

//   /** Loop on it
//    */
//   var_vector ops;
//   ops.push_back( Var(loop_expr) );
//   ops.push_back( Var(Op::LOOP) );


//  return ops;
}


var_vector doWhileNode::compile( Ref<Block> block, Binding &binding ) const {

  return var_vector();
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
  
  var_vector mops;
  var_vector ops;
  Ref<Block> tryb = new Block( "" );
  binding.startBlock();

  for (vector<Catch>::const_iterator x = catchers.begin(); 
       x != catchers.end(); x++) {

    /** Block Error CATCH<var_no>
     */
    ops.push_back( Var(x->branch->compile_to_expr( binding )) );
    ops.push_back( x->err );
    
    ops.push_back( Var( Op( Op::CATCH, binding.lookup( x->ident ) ) ) );
  }
  var_vector bops = do_branch->compile( tryb, binding );
  ops.insert( ops.end(), bops.begin(), bops.end() );
  tryb->code = ops;
  tryb->add_scope = binding.finishBlock();

  mops.push_back( Var(tryb) );
  mops.push_back( Var(Op::EVAL) );
  return mops;
}

var_vector ifNode::compile( Ref<Block> block, Binding &binding ) const {
  // Program: truth-expr exec-success IF

  /** Push the truth test first
   */
  var_vector ops = testExpr->compile( block, binding );
    
  /** Push the the success branch
   */
  Ref<Block> succ = trueBranch->compile_to_expr( binding );
  ops.push_back( Var(succ) );

  /** Push IF opcode
   */
  ops.push_back( Var(Op::IF ));

  return ops;
}
    
var_vector ifElseNode::compile( Ref<Block> block, Binding &binding ) const {
  // Program: truth-expr exec-fail exec-success IF

  /** Push the truth test first
   */
  var_vector ops = testExpr->compile( block, binding );
    
  /** Push the the success branch
   */
  Ref<Block> succ = trueBranch->compile_to_expr( binding );
  ops.push_back( Var(succ) );

  /** Push the fail branch 
   */
  Ref<Block> fail = elseBranch->compile_to_expr( binding );
  ops.push_back( Var(fail) );

  /** Push IFELSE opcode
   */
  ops.push_back( Var(Op::IFELSE ));
  return ops;
}



var_vector noopNode::compile( Ref<Block> block, Binding &binding ) const {
  var_vector a;
  return a;
}

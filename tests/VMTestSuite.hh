/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef VMTEST
#define VMTEST

#include "Block.hh"

/** Test lower level functionality of the VM -- opcodes, stack, 
 *  etc.
 */
class VMTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( VMTest );

  CPPUNIT_TEST( testPushPop );
  CPPUNIT_TEST( testRunHaltStopBlock );

  CPPUNIT_TEST( testOpNeg );
  CPPUNIT_TEST( testOpAdd );
  CPPUNIT_TEST( testOpSub );
  CPPUNIT_TEST( testOpMul );
  CPPUNIT_TEST( testOpDiv );
  CPPUNIT_TEST( testOpMod );

  CPPUNIT_TEST( testOpIsA );
  CPPUNIT_TEST( testOpEqual );
  CPPUNIT_TEST( testOpNequal );

  CPPUNIT_TEST( testOpLessT );
  CPPUNIT_TEST( testOpGreaterT );
  CPPUNIT_TEST( testOpLessTE );
  CPPUNIT_TEST( testOpGreaterTE );

  CPPUNIT_TEST( testOpAnd );
  CPPUNIT_TEST( testOpOr );
  CPPUNIT_TEST( testOpXor );
  CPPUNIT_TEST( testOpLshift );
  CPPUNIT_TEST( testOpRshift );
  CPPUNIT_TEST( testOpBand );
  CPPUNIT_TEST( testOpBor );
  CPPUNIT_TEST( testOpNot );

  CPPUNIT_TEST( testOpSelf );
  CPPUNIT_TEST( testOpCaller );
  CPPUNIT_TEST( testOpSource );
  CPPUNIT_TEST( testOpSelector );
  CPPUNIT_TEST( testOpArgs );

  CPPUNIT_TEST_SUITE_END();

public:
  
  VMTest()
    : CppUnit::TestFixture(),
      closure(0)
  {
  }

  void setUp()
  {
    /** Create an object representing self, etc.
     */
    self = Object::create();

    /** Create a message that represents the invocation for our
     *  closure
     */
    var_vector args;
    msg = new Message( (Task*)0, 0, 0, 0,
		       self, self, self, self, 
		       Symbol::create("vm_test"),
		       args );

    Ref<Message> x( msg->asRef<Message>() );
    closure = new Closure( x, self, 0 );
  }

 
  void tearDown()
  {
  }

private:
  Ref<Closure> closure;
  Var self;
  Var msg;

protected:

  void testPushPop() {
    closure->push( 5 );
    closure->push( "test string" );
    closure->push( Symbol::create( "symbol" ) );
    
    CPPUNIT_ASSERT( closure->stack.size() == 3 );
    CPPUNIT_ASSERT( closure->pop() == Symbol::create("symbol") );
    CPPUNIT_ASSERT( closure->pop() == Var("test string") );
    CPPUNIT_ASSERT( closure->pop() == Var(5) );
    CPPUNIT_ASSERT( closure->stack.size() == 0 );
  };

  void testRunHaltStopBlock() {
    closure->run();
    CPPUNIT_ASSERT( closure->ex_state == Closure::RUNNING );
    closure->halt();
    CPPUNIT_ASSERT( closure->ex_state == Closure::HALTED );
    closure->stop();
    CPPUNIT_ASSERT( closure->ex_state == Closure::STOPPED );
    closure->block();
    CPPUNIT_ASSERT( closure->ex_state == Closure::BLOCKED );
    closure->run();
    CPPUNIT_ASSERT( closure->ex_state == Closure::RUNNING );
  }

  void testOpNeg() {
    closure->push( 5 );
    closure->op_neg();
    CPPUNIT_ASSERT( closure->pop() == Var(-5) );
  }

  void testOpAdd() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_add();
    CPPUNIT_ASSERT( closure->pop() == Var(10) );
  }

  void testOpSub() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_sub();
    CPPUNIT_ASSERT( closure->pop() == Var(00) );
  }

  void testOpMul() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_mul();
    CPPUNIT_ASSERT( closure->pop() == Var(25) );
  }

  void testOpDiv() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_div();
    CPPUNIT_ASSERT( closure->pop() == Var(1) );
  }

  void testOpMod() {
    closure->push( 5 );
    closure->push( 3 );
    closure->op_mod();
    CPPUNIT_ASSERT( closure->pop() == Var(2) );
  }

  void testOpIsA() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_isa();
    CPPUNIT_ASSERT( closure->pop() == true );
    closure->push( Var("test") );
    closure->push( 5 );
    closure->op_isa();
    CPPUNIT_ASSERT( closure->pop() == false );
  }

  void testOpEqual() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_equal();
    CPPUNIT_ASSERT( closure->pop() == true );
    closure->push( 5 );
    closure->push( 4 );
    closure->op_equal();
    CPPUNIT_ASSERT( closure->pop() == false );
  }

  void testOpNequal() {
    closure->push( 5 );
    closure->push( 5 );
    closure->op_nequal();
    CPPUNIT_ASSERT( closure->pop() == false );
    closure->push( 5 );
    closure->push( 4 );
    closure->op_nequal();
    CPPUNIT_ASSERT( closure->pop() == true );
  }

  void testOpLessT() {
    closure->push( 4 );
    closure->push( 5 );
    closure->op_lesst();
    CPPUNIT_ASSERT( closure->pop() == true );
    closure->push( 5 );
    closure->push( 4 );
    closure->op_lesst();
    CPPUNIT_ASSERT( closure->pop() == false );
  }

  void testOpGreaterT() {
    closure->push( 4 );
    closure->push( 5 );
    closure->op_greatert();
    CPPUNIT_ASSERT( closure->pop() == false );
    closure->push( 5 );
    closure->push( 4 );
    closure->op_greatert();
    CPPUNIT_ASSERT( closure->pop() == true );
  }

  void testOpLessTE() {
    closure->push( 4 );
    closure->push( 5 );
    closure->op_lesste();
    CPPUNIT_ASSERT( closure->pop() == true );
    closure->push( 5 );
    closure->push( 4 );
    closure->op_lesste();
    CPPUNIT_ASSERT( closure->pop() == false );
  }

  void testOpGreaterTE() {
    closure->push( 4 );
    closure->push( 5 );
    closure->op_greaterte();
    CPPUNIT_ASSERT( closure->pop() == false );
    closure->push( 5 );
    closure->push( 4 );
    closure->op_greaterte();
    CPPUNIT_ASSERT( closure->pop() == true );
  }

  void testOpAnd() {
    closure->push( 1 );
    closure->push( 0 );
    closure->op_and();
    CPPUNIT_ASSERT( closure->pop() == Var(0) );
  }

  void testOpOr() {
    closure->push( 1 );
    closure->push( 0 );
    closure->op_or();
    CPPUNIT_ASSERT( closure->pop() == Var(1) );
  }

  void testOpXor() {
    closure->push( 2 );
    closure->push( 1 );
    closure->op_xor();
    CPPUNIT_ASSERT( closure->pop() == Var(3) );
  }

  void testOpLshift() {
    closure->push( 6 );
    closure->push( 2 );
    closure->op_lshift();
    CPPUNIT_ASSERT( closure->pop() == Var(24) );
  }

  void testOpRshift() {
    closure->push( 32 );
    closure->push( 2 );
    closure->op_rshift();
    CPPUNIT_ASSERT( closure->pop() == Var(8) );
  }

  void testOpBand() {
    closure->push( 34 );
    closure->push( 2 );
    closure->op_band();
    CPPUNIT_ASSERT( closure->pop() == Var(2) );
  }

  void testOpBor() {
    closure->push( 34 );
    closure->push( 6 );
    closure->op_bor();
    CPPUNIT_ASSERT( closure->pop() == Var(38) );
  }

  void testOpNot() {
    closure->push( 1 );
    closure->op_not();
    CPPUNIT_ASSERT( closure->pop() == false );
    closure->push( 0 );
    closure->op_not();
    CPPUNIT_ASSERT( closure->pop() == true );
  }

  void testOpSelf() {
    closure->op_self();
    CPPUNIT_ASSERT( closure->pop() == closure->self );
  }

  void testOpCaller() {
    closure->op_caller();
    CPPUNIT_ASSERT( closure->pop() == closure->caller );
  }

  void testOpSource() {
    closure->op_source();
    CPPUNIT_ASSERT( closure->pop() == closure->source );
  }

  void testOpSelector() {
    closure->op_selector();
    CPPUNIT_ASSERT( closure->pop() == closure->selector );
  }

  void testOpArgs() {
    closure->op_args();
    CPPUNIT_ASSERT( closure->pop() == Var(List::from_vector
( closure->args )) );
  }



};

#endif /* STRINGTEST */


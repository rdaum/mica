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
      frame(0)
  {
  }

  void setUp()
  {
    /** Create an object representing self, etc.
     */
    self = Object::create();

    /** Create a message that represents the invocation for our
     *  frame
     */
    var_vector args;
    msg = new Message( (Task*)0, 0, 0, 0,
		       self, self, self, self, 
		       Symbol::create("vm_test"),
		       args );

    Ref<Message> x( msg->asRef<Message>() );
    frame = new Frame( x, self, 0 );
  }

 
  void tearDown()
  {
  }

private:
  Ref<Frame> frame;
  Var self;
  Var msg;

protected:

  void testPushPop() {
    frame->push( 5 );
    frame->push( "test string" );
    frame->push( Symbol::create( "symbol" ) );
    
    CPPUNIT_ASSERT( frame->stack.size() == 3 );
    CPPUNIT_ASSERT( frame->pop() == Symbol::create("symbol") );
    CPPUNIT_ASSERT( frame->pop() == Var("test string") );
    CPPUNIT_ASSERT( frame->pop() == Var(5) );
    CPPUNIT_ASSERT( frame->stack.size() == 0 );
  };

  void testRunHaltStopBlock() {
    frame->run();
    CPPUNIT_ASSERT( frame->ex_state == Frame::RUNNING );
    frame->halt();
    CPPUNIT_ASSERT( frame->ex_state == Frame::HALTED );
    frame->stop();
    CPPUNIT_ASSERT( frame->ex_state == Frame::STOPPED );
    frame->block();
    CPPUNIT_ASSERT( frame->ex_state == Frame::BLOCKED );
    frame->run();
    CPPUNIT_ASSERT( frame->ex_state == Frame::RUNNING );
  }

  void testOpNeg() {
    frame->push( 5 );
    frame->op_neg();
    CPPUNIT_ASSERT( frame->pop() == Var(-5) );
  }

  void testOpAdd() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_add();
    CPPUNIT_ASSERT( frame->pop() == Var(10) );
  }

  void testOpSub() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_sub();
    CPPUNIT_ASSERT( frame->pop() == Var(00) );
  }

  void testOpMul() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_mul();
    CPPUNIT_ASSERT( frame->pop() == Var(25) );
  }

  void testOpDiv() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_div();
    CPPUNIT_ASSERT( frame->pop() == Var(1) );
  }

  void testOpMod() {
    frame->push( 5 );
    frame->push( 3 );
    frame->op_mod();
    CPPUNIT_ASSERT( frame->pop() == Var(2) );
  }

  void testOpIsA() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_isa();
    CPPUNIT_ASSERT( frame->pop() == true );
    frame->push( Var("test") );
    frame->push( 5 );
    frame->op_isa();
    CPPUNIT_ASSERT( frame->pop() == false );
  }

  void testOpEqual() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_equal();
    CPPUNIT_ASSERT( frame->pop() == true );
    frame->push( 5 );
    frame->push( 4 );
    frame->op_equal();
    CPPUNIT_ASSERT( frame->pop() == false );
  }

  void testOpNequal() {
    frame->push( 5 );
    frame->push( 5 );
    frame->op_nequal();
    CPPUNIT_ASSERT( frame->pop() == false );
    frame->push( 5 );
    frame->push( 4 );
    frame->op_nequal();
    CPPUNIT_ASSERT( frame->pop() == true );
  }

  void testOpLessT() {
    frame->push( 4 );
    frame->push( 5 );
    frame->op_lesst();
    CPPUNIT_ASSERT( frame->pop() == true );
    frame->push( 5 );
    frame->push( 4 );
    frame->op_lesst();
    CPPUNIT_ASSERT( frame->pop() == false );
  }

  void testOpGreaterT() {
    frame->push( 4 );
    frame->push( 5 );
    frame->op_greatert();
    CPPUNIT_ASSERT( frame->pop() == false );
    frame->push( 5 );
    frame->push( 4 );
    frame->op_greatert();
    CPPUNIT_ASSERT( frame->pop() == true );
  }

  void testOpLessTE() {
    frame->push( 4 );
    frame->push( 5 );
    frame->op_lesste();
    CPPUNIT_ASSERT( frame->pop() == true );
    frame->push( 5 );
    frame->push( 4 );
    frame->op_lesste();
    CPPUNIT_ASSERT( frame->pop() == false );
  }

  void testOpGreaterTE() {
    frame->push( 4 );
    frame->push( 5 );
    frame->op_greaterte();
    CPPUNIT_ASSERT( frame->pop() == false );
    frame->push( 5 );
    frame->push( 4 );
    frame->op_greaterte();
    CPPUNIT_ASSERT( frame->pop() == true );
  }

  void testOpAnd() {
    frame->push( 1 );
    frame->push( 0 );
    frame->op_and();
    CPPUNIT_ASSERT( frame->pop() == Var(0) );
  }

  void testOpOr() {
    frame->push( 1 );
    frame->push( 0 );
    frame->op_or();
    CPPUNIT_ASSERT( frame->pop() == Var(1) );
  }

  void testOpXor() {
    frame->push( 2 );
    frame->push( 1 );
    frame->op_xor();
    CPPUNIT_ASSERT( frame->pop() == Var(3) );
  }

  void testOpLshift() {
    frame->push( 6 );
    frame->push( 2 );
    frame->op_lshift();
    CPPUNIT_ASSERT( frame->pop() == Var(24) );
  }

  void testOpRshift() {
    frame->push( 32 );
    frame->push( 2 );
    frame->op_rshift();
    CPPUNIT_ASSERT( frame->pop() == Var(8) );
  }

  void testOpBand() {
    frame->push( 34 );
    frame->push( 2 );
    frame->op_band();
    CPPUNIT_ASSERT( frame->pop() == Var(2) );
  }

  void testOpBor() {
    frame->push( 34 );
    frame->push( 6 );
    frame->op_bor();
    CPPUNIT_ASSERT( frame->pop() == Var(38) );
  }

  void testOpNot() {
    frame->push( 1 );
    frame->op_not();
    CPPUNIT_ASSERT( frame->pop() == false );
    frame->push( 0 );
    frame->op_not();
    CPPUNIT_ASSERT( frame->pop() == true );
  }

  void testOpSelf() {
    frame->op_self();
    CPPUNIT_ASSERT( frame->pop() == frame->self );
  }

  void testOpCaller() {
    frame->op_caller();
    CPPUNIT_ASSERT( frame->pop() == frame->caller );
  }

  void testOpSource() {
    frame->op_source();
    CPPUNIT_ASSERT( frame->pop() == frame->source );
  }

  void testOpSelector() {
    frame->op_selector();
    CPPUNIT_ASSERT( frame->pop() == frame->selector );
  }

  void testOpArgs() {
    frame->op_args();
    CPPUNIT_ASSERT( frame->pop() == Var(List::from_vector
( frame->args )) );
  }



};

#endif /* STRINGTEST */


/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef VARTEST_HH
#define VARTEST_HH

class VarTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( VarTest );
  CPPUNIT_TEST( testCompareInt );
  CPPUNIT_TEST( testAddInt );
  CPPUNIT_TEST( testCompareFloat );
  CPPUNIT_TEST( testAssignFloat );
  CPPUNIT_TEST( testAssignOpCode );
  CPPUNIT_TEST( testCompareOpCode );
  CPPUNIT_TEST( testCopyOpCode );
  CPPUNIT_TEST( testAddFloat );
  CPPUNIT_TEST( testPowXor );
  CPPUNIT_TEST( testCompareNone );
  CPPUNIT_TEST( testAssignNone );
  CPPUNIT_TEST( testRefcnt );
  CPPUNIT_TEST( testCharCompare );
  CPPUNIT_TEST( testTypeOf );
  CPPUNIT_TEST( testTruth );
  CPPUNIT_TEST( testAndOr );
  CPPUNIT_TEST_SUITE_END();

protected:
  void testCompareInt() {
    CPPUNIT_ASSERT( mica::Var(1) == mica::Var(1) );
  };

  void testAssignInt() {
    mica::Var x = 5;
    CPPUNIT_ASSERT( x == 5 );
  }

  void testPowXor() {
    mica::Var x = 5;
    mica::Var y = 2;
    CPPUNIT_ASSERT( (x ^ y) == (5 ^ 2) );
  }

  void testAddInt() {
    CPPUNIT_ASSERT( (mica::Var(5) + mica::Var(5)) == 10 );
  }

  void testCompareFloat() {
    CPPUNIT_ASSERT( mica::Var(1.3) == 1.3 );
  };

  void testAssignFloat() {
    mica::Var x = 5.2;
    CPPUNIT_ASSERT( x == 5.2 );
  }

  void testAssignOpCode() {
    mica::Var x = mica::RETURN;
    CPPUNIT_ASSERT( x == mica::RETURN );
  }


  void testCompareOpCode() {
    mica::Var x = mica::ADD;
    CPPUNIT_ASSERT( x == mica::ADD );
    CPPUNIT_ASSERT( x != mica::Var(mica::SUB) );
    CPPUNIT_ASSERT( x != mica::Var((int)mica::ADD) );
  }

  void testCopyOpCode() {
    mica::Var x = mica::RETURN;
    mica::Var y = x;
    CPPUNIT_ASSERT( y == mica::RETURN );
  }

  void testAddFloat() {
    CPPUNIT_ASSERT( (mica::Var(5.3) + mica::Var(5.3)) == 10.6 );
  }

  void testCompareNone() {
    CPPUNIT_ASSERT( mica::Var() == mica::Var() );
  };

   void testAssignNone() {

     // This test no longer works because None's constructor is
     // now private.

     // mica::Var x( new  mica::None() );
     // CPPUNIT_ASSERT( x == mica::Var(new  mica::None()) );

  };

  void testRefcnt() {
//     mica::String *x = new  mica::String("test"); 
//     {
//       mica::Var z = x;
//     }
//     CPPUNIT_ASSERT( x->guard != 0xcafebabe );
  };

  void testCharCompare() {
    mica::Var x = 'x';
    CPPUNIT_ASSERT( x == mica::Var('x') );
  };

  void testTypeOf() {
    mica::Var x = "my string";
    CPPUNIT_ASSERT( x.typeOf() != typeid(mica::None) );
  };

  void testTruth() {
    mica::Var x = 1;
    mica::Var y = 0;
    CPPUNIT_ASSERT( x.truth() == true );
    CPPUNIT_ASSERT( y.truth() == false );

    x = "test";
    CPPUNIT_ASSERT( x.truth() == true );

    mica::Var z;
    CPPUNIT_ASSERT( z.truth() == false );
 }

  void testAndOr() {
    mica::Var x = 1;
    mica::Var y = 0;
    CPPUNIT_ASSERT( (x && y) == mica::Var(0) );
    CPPUNIT_ASSERT( (x || y) == mica::Var(1) );
    CPPUNIT_ASSERT( (y && x) == mica::Var(0) );
    CPPUNIT_ASSERT( (y || x) == mica::Var(1) );

    y = 2;
    CPPUNIT_ASSERT( (x && y) == mica::Var(2) );
    CPPUNIT_ASSERT( (x || y) == mica::Var(1) );
    CPPUNIT_ASSERT( (y && x) == mica::Var(1) );
    CPPUNIT_ASSERT( (y || x) == mica::Var(2) );

    x = "test";
    y = 0;  
    CPPUNIT_ASSERT( (x && y) == mica::Var(0) );
    CPPUNIT_ASSERT( (x || y) == mica::Var("test") );

    y = 1;
    CPPUNIT_ASSERT( (x && y) == mica::Var(1) );
    CPPUNIT_ASSERT( (x || y) == mica::Var("test") );

  }

};

#endif /* VARTEST */

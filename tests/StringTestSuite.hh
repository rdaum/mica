/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef STRINGTEST
#define STRINGTEST

class StringTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( StringTest );
  CPPUNIT_TEST( testStringCompare );
  CPPUNIT_TEST( testStringAppend ) ;
  CPPUNIT_TEST( testStringLength );
  CPPUNIT_TEST( testStringIndex );
  CPPUNIT_TEST( testStringSlice );
  CPPUNIT_TEST( testStringReplace );
  CPPUNIT_TEST( testStringReplaceSlice );
  CPPUNIT_TEST( testStringInsert );
  CPPUNIT_TEST( testStringInsertStr );
  CPPUNIT_TEST( testStringErase );
  CPPUNIT_TEST( testStringEraseRange );
  CPPUNIT_TEST( testStringFind );
  CPPUNIT_TEST_SUITE_END();

protected:
 
  void testStringCompare() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x == mica::Var(new  mica::String("test")) );
  };

  void testStringAppend() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x + x  == mica::Var(new  mica::String("testtest")));
  };

  void testStringLength() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x.length() == 4 );
  };

  void testStringIndex() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x.getItem(2) == mica::Var("s"));
    CPPUNIT_ASSERT( x[1] == mica::Var("e") );
  };

  void testStringSlice() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x.getSlice(1,2) == mica::Var("es"));
  }

  void testStringReplace() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x->replace(1, mica::Var('f')) == mica::Var("tfst"));
  }

  void testStringReplaceSlice() {
    mica::Var x = new  mica::String("testing");
    mica::Var y = x->replace(1, 3, "each");
    CPPUNIT_ASSERT( y == mica::Var("teacing"));
  }

  void testStringInsert() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x->insert(1, mica::Var('f')) == mica::Var("tfest"));
  }

  void testStringInsertStr() {
    mica::Var x = new  mica::String("test");
    CPPUNIT_ASSERT( x->insert(1, mica::Var("fe")) == mica::Var("tfeest"));
  }

  void testStringErase() {
    mica::Var x = "test";
    CPPUNIT_ASSERT( x->erase(2) == Var("tet") );
  }
  void testStringEraseRange() {
    mica::Var x = "testing";
    CPPUNIT_ASSERT( x->erase(2,2) == Var("teing") );
  }


  void testStringFind() {
    mica::Var x = "testing";
    CPPUNIT_ASSERT( x->find("est" ) == 1 );
    CPPUNIT_ASSERT( x->find("bo") == Var() );
    CPPUNIT_ASSERT( x->find('s') == 2 );
  }

};

#endif /* STRINGTEST */


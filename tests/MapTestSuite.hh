/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef MAPTEST
#define MAPTEST

class MapTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( MapTest );
  CPPUNIT_TEST( testMapCompare );
  CPPUNIT_TEST( testMapCopy );
  CPPUNIT_TEST( testMapLength );
  CPPUNIT_TEST( testMapIndex );
  CPPUNIT_TEST( testMapReplace );
  CPPUNIT_TEST( testMapErase );
  CPPUNIT_TEST( testMapFind );
  CPPUNIT_TEST_SUITE_END();

protected:

  void testMapCompare() {
    Var x = new  Map();
    CPPUNIT_ASSERT( x == Var(new  Map()) );
  };

  void testMapCopy() {
    Var x = new Map();
    x->insert("a", 5);
    x->insert(1, 2);
    Var y = new Map( *(x->asType<Map*>()) );
    CPPUNIT_ASSERT( x[Var("a")] == Var(5) );
    CPPUNIT_ASSERT( x[Var(1)] == Var(2) );

    var_map mm;
    mm.insert( make_pair( Var(1), Var(2) ) );
    mm[Var(1)] = Var(2);
    Var z = new Map( mm );
    CPPUNIT_ASSERT( z[Var(1)] == Var(2) );
  }

  void testMapLength() {
    Var x = new  Map();
    x->insert("a", 5);
    
    CPPUNIT_ASSERT( x.length() == 1 );

  };

  void testMapIndex() {
    Var x = new  Map();

    x->insert( "key", "value" );
    x->insert( "mommy", "dad" );
    x->insert( Var(1), Var(2) );

    CPPUNIT_ASSERT( x.getItem("key") == Var("value"));
    CPPUNIT_ASSERT( x[Var("mommy")] == Var("dad") );
    CPPUNIT_ASSERT( x[Var(1)] == Var(2) );
  };

  void testMapReplace() {
    Var x = new  Map();
    x = x->insert( "key", "value" );
    x = x->insert( "mom", "dad" );

    x = x->replace( "key", Var("tom"));
    CPPUNIT_ASSERT( x[Var("key")] == Var("tom"));
  }


  void testMapErase() {
    Var x = new  Map();
    x = x->insert( "key", "value" );
    x = x->insert( "mom", "dad" );

    x->erase("key");

    CPPUNIT_ASSERT( x->length() == 1 );

  }

  void testMapFind() {
    Var x = new  Map();
    x = x->insert( "key", "value" );
    x = x->insert( "mom", "dad" );

    CPPUNIT_ASSERT( x->find("key") == Var("value") );
    CPPUNIT_ASSERT( x->find("job") == Var() );

  }

};

#endif /* MAPTEST */


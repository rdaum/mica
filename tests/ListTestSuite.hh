/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef LISTTEST
#define LISTTEST

class ListTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( ListTest );

  CPPUNIT_TEST( testListCompare );
  CPPUNIT_TEST( testListAppend );
  CPPUNIT_TEST( testListLength );
  CPPUNIT_TEST( testListIndex );
  CPPUNIT_TEST( testListSlice );
  CPPUNIT_TEST( testListReplace );
  CPPUNIT_TEST( testListReplaceSlice );
  CPPUNIT_TEST( testListInsert );
  CPPUNIT_TEST( testListErase );
  CPPUNIT_TEST( testListEraseRange );
  CPPUNIT_TEST( testListFind );

  CPPUNIT_TEST_SUITE_END();

protected:
 
   void testListCompare() {

    Var x = List::empty();
    Var y = List::empty();
    CPPUNIT_ASSERT( x == Var(List::empty()) );
  
    x = x + Var(5);
    y = y + Var(5);
    CPPUNIT_ASSERT( x == y );
    x = x + Var("test");
    y = y + Var("test");
    CPPUNIT_ASSERT( x == y );
    x = x + Var(3);
    CPPUNIT_ASSERT( x != y );
  };

  void testListAppend() {
    Var x = List::empty();
    Var y = 5;
    var_vector z;
    z.push_back(y);
  
    CPPUNIT_ASSERT( x + y == Var(List::from_vector
(z)));
  };

  void testListLength() {
    Var x = List::empty();
    x = x + Var("test");
    x = x + Var(3);
    CPPUNIT_ASSERT( x.length() == 2 );
  };

  void testListIndex() {
    Var x = List::empty();
    x = x + Var(5);
    x = x + Var('s');
    x = x + Var("frankly");

    CPPUNIT_ASSERT( x.getItem(1) == Var('s'));
    CPPUNIT_ASSERT( x[2] == Var("frankly") );
  };

  void testListSlice() {
    Var x = List::empty();
    x = x + Var(5);
    x = x + Var("test");
    x = x + Var(32);
    Var y = x.getSlice(1,2);
    CPPUNIT_ASSERT( y[0] == Var("test") && y[1] == Var(32));
  }

  void testListReplace() {
    Var x = List::empty();
    x = x + Var(5);
    x = x + Var('s');
    x = x + Var(32);
    x = x->replace(1, Var('f'));
    CPPUNIT_ASSERT( x[1] == Var('f'));
  }

  void testListReplaceSlice() {
    Var x = List::empty();
    x = x + Var('a');
    x = x + Var('b');
    x = x + Var('c');
    x = x + Var('d');
    x = x + Var('e');
    x = x + Var('f');
    x = x + Var('g');

    Var y = List::empty();
    y = y + Var(1);
    y = y + Var(2);
    y = y + Var(3);
    y = y + Var(4);

    Var z = x->replace(1, 3, y );

    CPPUNIT_ASSERT( z[0] == Var('a') &&
		    z[1] == Var(1) &&
		    z[2] == Var(2) &&
		    z[4] == Var('e') &&
		    z->length() == x->length() );

  }

  void testListInsert() {
    Var x = List::empty();
    x = x + Var('a');
    x = x + Var('b');
    x = x + Var('c');

    Var y = x->insert(1, Var('f'));

    CPPUNIT_ASSERT( y[0] == Var('a') && 
		    y[1] == Var('f') &&
		    y[2] == Var('b') );
  }

  void testListErase() {
    Var x = List::empty();
    x = x + Var('a');
    x = x + Var('b');
    x = x + Var('c');
    x = x + Var('d');

    Var z = x->erase(2);

    CPPUNIT_ASSERT( z[0] == Var('a') &&
		    z[1] == Var('b') &&
		    z[2] == Var('d') &&
		    z->length() == x->length() - 1 );

  }

  void testListEraseRange() {
    Var x = List::empty();
    x = x + Var('a');
    x = x + Var('b');
    x = x + Var('c');
    x = x + Var('d');

    Var z = x->erase(1,1);
  
    CPPUNIT_ASSERT( z[0] == Var('a') &&
		    z[1] == Var('d') &&
		    z->length() == 2 );

  }

  void testListFind() {
    Var x = List::empty();
    x = x + Var('a');
    x = x + Var('b');
    x = x + Var('c');
    x = x + Var('d');

    CPPUNIT_ASSERT( x->find('a') == 0 );
    CPPUNIT_ASSERT( x->find('c') == 2 );


    CPPUNIT_ASSERT( x->find('s') == Var() );
  }

};

#endif /* LISTTEST */


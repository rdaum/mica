/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef OBJECTTEST
#define OBJECTTEST

class ObjectTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( ObjectTest );

  CPPUNIT_TEST( testObjectCompare );
  CPPUNIT_TEST( testObjectDeclare );
  CPPUNIT_TEST( testObjectAssign );
  CPPUNIT_TEST( testObjectGet );
  CPPUNIT_TEST( testObjectOptSlots );
  CPPUNIT_TEST( testObjectClone );
  CPPUNIT_TEST( testObjectInherit );
  CPPUNIT_TEST( testObjectMoreInherit );
  CPPUNIT_TEST( testObjectMultipleInherit );
  CPPUNIT_TEST( testObjectOverload );

  CPPUNIT_TEST_SUITE_END();

protected:
 
  void testObjectCompare() {
    /** Two objects should not be the same just because their members
	are.  Each object is unique...
    */
    Var x = Object::create();
    CPPUNIT_ASSERT( x != Var(Object::create()) );
  };

  void testObjectDeclare() {
    Var x = Object::create();
    x->declare( x, Symbol::create("test"), 5);
    CPPUNIT_ASSERT( x->length() == 2 );
  }

  void testObjectAssign() {
    Var x = Object::create();
    x->declare( x, Symbol::create("test"), 5);
    x->assign( x, Symbol::create("test"), 5);
    CPPUNIT_ASSERT( x->length() == 2 );
  };

  void testObjectGet() {
    Var x = Object::create();
    x->declare( x, Symbol::create("test"), 5);
    Var y = x.get( x, Symbol::create("test"));
    Var z = y->value();
    
    CPPUNIT_ASSERT(y->value() == z);
    CPPUNIT_ASSERT(y->value() == Var(5));

  };

  void testObjectOptSlots() {
    Var x = Object::create();
    x->declare( x, Symbol::create("test"), 5);
    x->declare( x, Symbol::create("test2"), "woah");
    
    CPPUNIT_ASSERT( x->slots().length() == 3 );
  }

  void testObjectRemove() {
    Var x = Object::create();
    x->declare( x, Symbol::create("test"), 5);
    x->assign( x, Symbol::create("test2"), "woah");
    x->remove( x, Symbol::create("test2"));
    CPPUNIT_ASSERT( x->slots().length() == 3 );
  }

  void testObjectClone() {
    Var x = Object::create();
    Var y = x->clone();
    CPPUNIT_ASSERT( y != x );
  }

  void testObjectInherit() {
    Var x = Object::create();
    x.declare( x, Symbol::create("test"), 5 );
    Var y = x.clone();
    
    CPPUNIT_ASSERT( y.get( x, Symbol::create("test") )->value() == Var(5) );
  }

  void testObjectOverload() {
    Var x = Object::create();
    x->declare( x, Symbol::create("test"), 5);
    Var y = x->clone();
    y->assign( x, Symbol::create("test"), "bob");

    CPPUNIT_ASSERT( y.get(x, Symbol::create("test"))->value() == Var("bob") );
    CPPUNIT_ASSERT( x.get(x, Symbol::create("test"))->value() == Var(5) );
  }

  void testObjectMoreInherit() {
    Var x(Object::create());
    x.declare( x, Symbol::create("test"), Var(5) );
    Var y = x.clone();
    Var z = y.clone();
    CPPUNIT_ASSERT( y.get(x, Symbol::create("test"))->value() == Var(5) );
    CPPUNIT_ASSERT( z.get(x, Symbol::create("test"))->value() == Var(5) );
  }

  void testObjectMultipleInherit() {
    Var x = Object::create();  
    Var z = Object::create();

    x->declare( x, Symbol::create("test"), 5);
    z->declare( z, Symbol::create("test"), "charles");
    z->declare( z, Symbol::create("mojo"), 666);

    /** Get a child of x
     */
    Var y = x->clone();    

    CPPUNIT_ASSERT( y.get( x, Symbol::create("test"))->value() == Var(5) );

    /** Add z to its delegates
     */
    Var p = y.get( y, DELEGATES_SYM)->value();    
    p = p + z;

    y->assign( y, DELEGATES_SYM, p);

    CPPUNIT_ASSERT( y.get( x, Symbol::create("test"))->value() == Var(5) );
    CPPUNIT_ASSERT( y.get( z, Symbol::create("mojo"))->value() == Var(666) );


  }

};

#endif /* OBJECTTEST */


/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef SERIALIZETESTS
#define SERIALIZETESTS

class SerializationTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( SerializationTest );

  CPPUNIT_TEST( testInt );
  CPPUNIT_TEST( testChar );
  CPPUNIT_TEST( testFloat );
  CPPUNIT_TEST( testOpCode );
  CPPUNIT_TEST( testString );
  CPPUNIT_TEST( testSymbol );
  CPPUNIT_TEST( testNone );
  CPPUNIT_TEST( testList );
  CPPUNIT_TEST( testMap );
  CPPUNIT_TEST( testObject );
  CPPUNIT_TEST( testIterator );
  CPPUNIT_TEST( testOptSlot );
  CPPUNIT_TEST( testBlock );
  CPPUNIT_TEST_SUITE_END();
 
protected:
  void testInt() {
    Var x = 5;

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

 
  void testChar() {
    Var x = 't';

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

 
  void testFloat() {
    Var x = 4.2;

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

 
  void testOpCode() {
    Var x = mica::SEND;

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

  void testString() {
    Var x = "a string test";

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

  void testSymbol() {
    Var x(Symbol::create("a string test"));

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

  void testNone() {
    Var x;

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );
  };

  void testList() {
    Var x = new  List();

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );

    x = x + Var(5);
    x = x + Var('3');
    x = x + Var("test");
    x = x + Var(Symbol::create("testing"));

    Unserializer p(x.serialize());

    Var z = p.parse();

    CPPUNIT_ASSERT( x.length() == z.length() );
    CPPUNIT_ASSERT( x[0] == z[0] );
    
    CPPUNIT_ASSERT( x == z );
  };

  void testMap() {
    Var x = new  Map();

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    CPPUNIT_ASSERT( x == y.parse() );

    x->insert(Var(5), Var("test"));
    x->insert(Var("yo"), Var("bob"));
    x->insert(Var(1), Var(2));

    s_form = x.serialize();
    Unserializer p(s_form);

    Var z = p.parse();

    CPPUNIT_ASSERT( z[Var(5)] == Var("test") );
    CPPUNIT_ASSERT( z[Var("yo")] == Var("bob") );
    CPPUNIT_ASSERT( z[Var(1)] == x[Var(1)] );
  };


 void testObject() {
   mica_string s_form; 

   /** We stick this in a block so that the refcount for x goes down,
    *   allowing us to recreate an object with that same ID.
    */
   {
     Var x = Object::create();

     x->declare( x, Symbol::create("test"), "bob");
     x->declare( x, Symbol::create("bob"), Var(32));

     s_form = x.serialize();
   }

   Unserializer y(s_form);

   Var z = y.parse();

   CPPUNIT_ASSERT( z->get(z, Symbol::create("test"))->value() == Var("bob") );

   CPPUNIT_ASSERT( z->get(z, Symbol::create("bob"))->value() == Var(32) );
  };


  void testIterator() {
    Var p = "testString";
    Var x = p.begin();

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    Var z = y.parse();

    CPPUNIT_ASSERT( x == z );

    x++;

    CPPUNIT_ASSERT( x != z );

    z++;

    CPPUNIT_ASSERT( x == z );

    x = x + Var(3);
    
    CPPUNIT_ASSERT( x != z );

    z = z + Var(3);

    CPPUNIT_ASSERT( x == z );
  };


  void testOptSlot() {
    Var object = Object::create();
    Symbol test_symbol(Symbol::create("test_symbol"));
    object.declare( object, 
		    test_symbol,
		    5);

    Var test_slot( object.get( object, test_symbol ) );

    mica_string s_form = test_slot.serialize();
    Unserializer unserializer(s_form);

    Var unserialized_test_slot(unserializer.parse());

    CPPUNIT_ASSERT( unserialized_test_slot == test_slot );
    CPPUNIT_ASSERT( unserialized_test_slot.value() == test_slot.value() );
    CPPUNIT_ASSERT( unserialized_test_slot.value() == Var(5) );
  };


  void testBlock() {
    Var x = new  Block("test");
    x->asType<Block*>()->code.push_back(mica::RETURN);

    mica_string s_form = x.serialize();

    Unserializer y(s_form);

    Var z = y.parse();

    Block *bl2 = z->asType<Block*>();
    Block *bl1 = x->asType<Block*>();

    CPPUNIT_ASSERT (bl2->code[0] == bl1->code[0]);
    CPPUNIT_ASSERT (bl2->code.size() == bl1->code.size());

  };


};

#endif /* SERIALIZETESTS */


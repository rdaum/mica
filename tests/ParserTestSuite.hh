/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef PARSERTEST
#define PARSERTEST

class ParserTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( ParserTest );

  CPPUNIT_TEST( testNumberLiteral );
  CPPUNIT_TEST( testFloatLiteral );
  CPPUNIT_TEST( testStringLiteral );
  CPPUNIT_TEST( testErrorLiteral );
  CPPUNIT_TEST( testErrorLiteral2 );
  CPPUNIT_TEST( testSymbolLiteral );
  CPPUNIT_TEST( testListLiteral );
  CPPUNIT_TEST( testMapLiteral );
  CPPUNIT_TEST( testBuiltinLiterals );

  CPPUNIT_TEST( testAddExpression );
  CPPUNIT_TEST( testSubExpression );
  CPPUNIT_TEST( testMulExpression );
  CPPUNIT_TEST( testDivExpression) ;
  CPPUNIT_TEST( testNotExpression );

  CPPUNIT_TEST( testAndExpression );
  CPPUNIT_TEST( testOrExpression );
  CPPUNIT_TEST( testEqualExpression );
  CPPUNIT_TEST( testUnequalExpression );
  CPPUNIT_TEST( testisAExpression );

  CPPUNIT_TEST( testBitAndExpresion );
  CPPUNIT_TEST( testBitOrExpresion );
  CPPUNIT_TEST( testBitXorExpresion );
  CPPUNIT_TEST( testLSExpression );
  CPPUNIT_TEST( testRSExpression );

  CPPUNIT_TEST( testNegExpression );
  CPPUNIT_TEST( testNegSubExpression );

  CPPUNIT_TEST( testGTExpression );
  CPPUNIT_TEST( testLTExpression );
  CPPUNIT_TEST( testGTEExpression );
  CPPUNIT_TEST( testLTEExpression );

  CPPUNIT_TEST( testParenthExpression );

  CPPUNIT_TEST( testVarDeclare );
  CPPUNIT_TEST( testVarAssign );
  CPPUNIT_TEST( testScatterVar );

  CPPUNIT_TEST( testReturn );
  CPPUNIT_TEST( testThrow );
  CPPUNIT_TEST( testNotify );
  CPPUNIT_TEST( testPublish );
  CPPUNIT_TEST( testCompose );
  CPPUNIT_TEST( testDestroy );
  CPPUNIT_TEST( testUnpublish );

  CPPUNIT_TEST( testPass1 );
  CPPUNIT_TEST( testPass2 );
  CPPUNIT_TEST( testPass3 );
  CPPUNIT_TEST( testPass4 );


  CPPUNIT_TEST( testIndexExpression);
  CPPUNIT_TEST( testPrivateSlotExpression);
  CPPUNIT_TEST( testPrivateSlotExpression2);
  CPPUNIT_TEST( testSelfPublicSlotExpression);
  CPPUNIT_TEST( testSelfPublicSlotExpression2);

  CPPUNIT_TEST( testRemovePrivate );
  CPPUNIT_TEST( testRemovePublic );

  CPPUNIT_TEST( testMessageSend);
  CPPUNIT_TEST( testMessageSend2);
  CPPUNIT_TEST( testMessageSendArgs);

  CPPUNIT_TEST( testBlockStatement );
  CPPUNIT_TEST( testLambdaLiteral );
  CPPUNIT_TEST( testMethodLiteral );

  CPPUNIT_TEST( testWhile );
  CPPUNIT_TEST( testIf1 );
  CPPUNIT_TEST( testIf2 );
  CPPUNIT_TEST( testIfElse );
  CPPUNIT_TEST( testIfElseIf );
  CPPUNIT_TEST( testIfElseIfElse );
  CPPUNIT_TEST( testBreak );

  CPPUNIT_TEST( testTryCatch );
  CPPUNIT_TEST( testFor );

  CPPUNIT_TEST( testComment );
  CPPUNIT_TEST( testComment2 );

  CPPUNIT_TEST_SUITE_END();

protected:

#define TEST_METHOD( NAME, STRING ) void test## NAME () { \
    Lexer l( STRING ); \
    Parser p(l); \
    try { \
      NPtr x = p.statement(); \
      CPPUNIT_ASSERT( x ); \
    } catch (Ref<Error> e) { \
      cerr << e << endl; \
      CPPUNIT_ASSERT(0); \
    } \
  }



  TEST_METHOD( NumberLiteral, "45;" );
  TEST_METHOD( FloatLiteral, "64.33;" );
  TEST_METHOD( ErrorLiteral, "~test;");
  TEST_METHOD( ErrorLiteral2, "~test(\"test\");");
  TEST_METHOD( StringLiteral, "\"this is a string\\n\\thola\";" );
  TEST_METHOD( SymbolLiteral, "#symbol;");
  TEST_METHOD( BuiltinLiterals, "return [self, selector, source, args, slots, id ];" );
  TEST_METHOD( ListLiteral, "[1, 2, 3, #symbol1, #symbol2, \"test\", [], #[], None, 54.3, ~err, self];");
  TEST_METHOD( MapLiteral, "#[ 1 => 2, 3 => 4, #test => \"strong\", [] =>  #[], 55.3 => ~err, self => sender ]; ");

  TEST_METHOD( AddExpression, "4 + 63;" );
  TEST_METHOD( SubExpression, "4 - 2;" );
  TEST_METHOD( MulExpression, "4 * 3;" );
  TEST_METHOD( DivExpression, "4 / 3;" );

  TEST_METHOD( NotExpression, "!1;" );

  TEST_METHOD( NegExpression, "-1;" );
  TEST_METHOD( NegSubExpression, "1 - -1;" );

  TEST_METHOD( AndExpression, "5 && 3;");
  TEST_METHOD( OrExpression, "5 || 2;");
  TEST_METHOD( EqualExpression, "5 == 5;");
  TEST_METHOD( UnequalExpression, "5 != 5;");
  TEST_METHOD( isAExpression, "5 isA 3;");

  TEST_METHOD( BitAndExpresion, "5 & 64;");
  TEST_METHOD( BitOrExpresion, "5 | 64;");
  TEST_METHOD( BitXorExpresion, "5 ^ 64;");

  TEST_METHOD( LSExpression, "5 << 3;");
  TEST_METHOD( RSExpression, "5 << 3;");

  TEST_METHOD( GTExpression, "5 > 4;" );
  TEST_METHOD( LTExpression, "4 < 5;" );
  TEST_METHOD( GTEExpression, "5 >= 4;" );
  TEST_METHOD( LTEExpression, "4 <= 5;" );

  TEST_METHOD( ParenthExpression, "4 + (3 / 2);" );

  TEST_METHOD( VarDeclare, "var a, b;");
  TEST_METHOD( VarAssign, "var a = 32;");
  TEST_METHOD( ScatterVar, "args => var (a,b,c);");

  TEST_METHOD( IndexExpression, "args[54];");

  TEST_METHOD( PrivateSlotExpression, ".buddy = 5;");
  TEST_METHOD( PrivateSlotExpression2, ".(#buddy) = 5;");
  TEST_METHOD( SelfPublicSlotExpression, ":test;");
  TEST_METHOD( SelfPublicSlotExpression2, ":(#test);");
  TEST_METHOD( MessageSend, "source:test();");
  TEST_METHOD( MessageSend2, "source:(#test)();");
  TEST_METHOD( MessageSendArgs, "source:test(1,2,3);");

  TEST_METHOD( RemovePrivate, "remove .test;  remove .(#test);");
  TEST_METHOD( RemovePublic, "remove :test; remove :(#test);");

  TEST_METHOD( Return, "return 54;");
  TEST_METHOD( Throw, "throw ~err;");
  TEST_METHOD( Notify, "notify \"test\";");
  TEST_METHOD( Publish, "publish 5 #test;");
  TEST_METHOD( Compose, "compose 5;");
  TEST_METHOD( Destroy, "destroy;");
  TEST_METHOD( Unpublish, "unpublish #test;");

  TEST_METHOD( Pass1, "pass;");
  TEST_METHOD( Pass2, "pass to source;");
  TEST_METHOD( Pass3, "pass(1,2);");
  TEST_METHOD( Pass4, "pass(1,2) to source;");

  TEST_METHOD( BlockStatement, "var a; { var b; a = 5; b = 3; } a = 3;" );
  TEST_METHOD( LambdaLiteral, "lambda<x> { return x; };");
  TEST_METHOD( MethodLiteral, "method<x> { return x; };");

  TEST_METHOD( While, "var x; while (5) { x = x + 1; }");
  TEST_METHOD( If1, "if (5) return 3;");
  TEST_METHOD( If2, "if (5) { return 3; }");
  TEST_METHOD( IfElse, "if (5) { return 3; } else return 3;");
  TEST_METHOD( IfElseIf, "if (5) { return 3; } else if (2) return 3;");
  TEST_METHOD( IfElseIfElse, "if (5) { return 3; } else if (2) return 3; else return 32;");

  TEST_METHOD( Break, "var x; while (5) { x = x + 1; break; }");

  TEST_METHOD( TryCatch, "var e; try { return 3; } catch (e ~err) { return 32; }");

  TEST_METHOD( For, "var x, y, z; for x in y { z = z + x; }");

  TEST_METHOD( Comment, "// test");
  TEST_METHOD( Comment2, "return 5; // test");

  virtual void registerTests(CppUnit::TestSuite *suite)
  {
  };



};

#endif /* PARSERTEST */


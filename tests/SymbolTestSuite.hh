/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef SYMBOLTEST
#define SYMBOLTEST

#include "common/mica.h"
#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
#  include <ext/hash_map>
#else
#  include <hash_map>
#endif

class SymbolTest
  : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE( SymbolTest );
  CPPUNIT_TEST( testSymbolCompareSame);
  CPPUNIT_TEST( testSymbolCompareUnsame);
  CPPUNIT_TEST( testSymbolCreateFromSymbol);  
  CPPUNIT_TEST( testSymbolHash);    
  CPPUNIT_TEST_SUITE_END();

protected:  
 
  void testSymbolCompareSame() {
    Var x(Symbol::create("testing"));
    CPPUNIT_ASSERT( x == Symbol::create("testing") );
  };

  void testSymbolCompareUnsame() {
    Var x(Symbol::create("test"));
    CPPUNIT_ASSERT( x != Var(Symbol::create("not test")));
  };

  void testSymbolCreateFromSymbol() {
    Var x(Symbol::create("test"));

    Var y(Symbol::create(x));

    CPPUNIT_ASSERT( x == y );
  }

  void testSymbolHash() {
    Var x(Symbol::create("test"));

    STD_EXT_NS::hash_map< Var, int, hash_var > z;

    z[x] = 5;

    CPPUNIT_ASSERT( z.find(x) != z.end() );

    Var k(Symbol::create("test"));

    CPPUNIT_ASSERT( z.find(k) != z.end() );

    int g = z.find(x)->second;

    CPPUNIT_ASSERT( g == 5 );
  }


};

#endif /* SYMBOLTEST */


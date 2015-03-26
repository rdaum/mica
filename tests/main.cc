/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/



#include <cppunit/TestSuite.h>
#include <cppunit/TextTestResult.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestCaller.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/XmlOutputter.h>
#include <cppunit/TextOutputter.h>
#include <cppunit/CompilerOutputter.h>


#include <iostream>
#include <math.h>

#include "Data.hh"
#include "Var.hh"

#include "String.hh"
#include "List.hh"
#include "Map.hh"
#include "Symbol.hh"

#include "Object.hh"
#include "Pool.hh"
#include "Pools.hh"
#include "MetaObjects.hh"
#include "Block.hh"
#include "GlobalSymbols.hh"
#include "Exceptions.hh"
#include "Unserializer.hh"

#include "MicaParser.hh"

using namespace std;
using namespace mica;

#include "VarTestSuite.hh"
#include "StringTestSuite.hh"
#include "ListTestSuite.hh"
#include "MapTestSuite.hh"
#include "SymbolTestSuite.hh"
#include "ObjectTestSuite.hh"
#include "SerializationTestSuite.hh"


CPPUNIT_TEST_SUITE_REGISTRATION( VarTest );
CPPUNIT_TEST_SUITE_REGISTRATION( StringTest );
CPPUNIT_TEST_SUITE_REGISTRATION( SymbolTest );
CPPUNIT_TEST_SUITE_REGISTRATION( ListTest );
CPPUNIT_TEST_SUITE_REGISTRATION( MapTest );
CPPUNIT_TEST_SUITE_REGISTRATION( ObjectTest );
CPPUNIT_TEST_SUITE_REGISTRATION( SerializationTest );

int main( int argc, char **argv )
{
  initSymbols();

  pair<PID, Var> pool_return = Pool::open( Symbol::create("builtin") ); 
  Pools::instance.setDefault( pool_return.first );
  
  MetaObjects::initialize( pool_return.second );

  CppUnit::TextUi::TestRunner runner;

  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();

  runner.addTest( registry.makeTest() );

  runner.setOutputter( new CppUnit::TextOutputter( &runner.result(),
						   std::cerr ) );

  runner.run();

  reference_counted::collect_cycles();

  return 0;
}


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
#include <cppunit/CompilerOutputter.h>


#include <iostream>
#include <math.h>

#include "Data.hh"
#include "Var.hh"
#include "List.hh"
#include "Symbol.hh"
#include "GlobalSymbols.hh"
#include "Object.hh"
#include "Pool.hh"
#include "Pools.hh"


#include "OpCodes.hh"
#include "Ref.hh"
#include "Task.hh"
#include "Message.hh"
#include "Closure.hh"


using namespace std;
using namespace mica;

#include "VMTestSuite.hh"

#include "MetaObjects.hh"

CPPUNIT_TEST_SUITE_REGISTRATION( VMTest );


int main( int argc, char **argv )
{
  initializeOpcodes();

  initSymbols();

  pair<PID, Var> pool_return = Pool::open( Symbol::create("builtin") ); 
  Pools::instance.setDefault( pool_return.first );
  
  MetaObjects::initialize( pool_return.second );

  CppUnit::TextUi::TestRunner runner;

  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();

  runner.addTest( registry.makeTest() );

  runner.setOutputter( CppUnit::CompilerOutputter::defaultOutputter( &runner.result(),
								     std::cerr ) );

  runner.run();

  return 0;
}


/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/


#include "Data.hh"
#include "Var.hh"

#include "Exceptions.hh"

#include "Atom.hh"
#include "Task.hh"
#include "Block.hh"
#include "Nodes.hh"
#include "MicaParser.hh"
#include "Symbol.hh"
#include "Error.hh"
#include "GlobalSymbols.hh"

#include "compile.hh"

#include <boost/spirit.hpp>

using namespace mica;


Ref<Block> mica::compile( mica_string source ) 
{
  micaParser parser(source.c_str());

  NPtr nodes;

  nodes = parser.parse();
  
  Binding binding;  
  Ref<Block> method(new (aligned) Block(source));
  
  method->code = nodes->compile( method, binding );
  
  return method;

}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "compile.hh"

#include <boost/spirit.hpp>

#include "parser/MicaParser.hh"
#include "parser/Nodes.hh"
#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "vm/Block.hh"
#include "vm/Task.hh"

using namespace mica;

Ref<Block> mica::compile(mica_string source) {
  micaParser parser(source.c_str());

  NPtr nodes;

  nodes = parser.parse();

  Binding binding;
  Ref<Block> method(new (aligned) Block(source));

  method->code = nodes->compile(method, binding);

  return method;
}

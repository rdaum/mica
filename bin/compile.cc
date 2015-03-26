/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "compile.hh"

#include <boost/spirit.hpp>

\#include "types/Atom.hh"
#include "types/Exceptions.hh"
#include "vm/Block.hh"
#include "vm/Task.hh"

namespace mica {

Ref<Block> mica::compile(mica_string source) {
  micaParser parser(source.c_str());

  NPtr nodes;

  nodes = parser.parse();

  Binding binding;
  Ref<Block> method(new Block(source));

  method->code = nodes->compile(method, binding);

  return method;
}

}  // namespace mica

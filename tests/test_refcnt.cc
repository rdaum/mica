#include "config.h"
#include "common/mica.h"

#include "Data.hh"
#include "Var.hh"
#include "Symbol.hh"
#include "GlobalSymbols.hh"
#include "MetaObjects.hh"
#include "Object.hh"
#include "Pool.hh"
#include "Pools.hh"
#include "Block.hh"
#include "Binding.hh"
#include "MicaParser.hh"
#include "OpCodes.hh"

using namespace mica;
using namespace std;


int main() {
  initSymbols();

  pair<PID, Var> pool_return = Pool::open( Symbol::create("builtin") );
  Pools::instance.setDefault( pool_return.first );

  MetaObjects::initialize( pool_return.second );
  mica_string program("remove .x;\n");
  micaParser parser(program);
  NPtr nodes;
  try {
    nodes = parser.parse();

    Ref<Block> block( new Block(program) );
    Binding binding;
    block->code = nodes->compile( block, binding );

    initializeOpcodes();
    for ( var_vector::iterator x = block->code.begin();
	  x != block->code.end(); x++ ) {
      if (x->type_identifier() == Type::OPCODE && x->toOpCode() >=0)
	cerr << opcodes[x->toOpCode()]->name << " ";
      else
	cerr << *x << " ";
    }
    cerr << endl;

  } catch (::parse_error pe) {
    cerr << "parse error in line #" << pe.line << " column #" << pe.column << endl;
  } catch (::lex_error le) {
    cerr << "lex error in line #" << le.line << " column #" << le.column << endl;
  } catch (const Ref<Error> &err) {
    cerr << err << endl;
  }


  
}

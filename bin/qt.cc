#include <iostream>

#include "Data.hh"
#include "Var.hh"
#include "Error.hh"
#include "Exceptions.hh"
#include "GlobalSymbols.hh"
#include "List.hh"

using namespace mica;
using namespace std;

int main() {

  Var x( String::create("test") );
  cerr << (void*)(x.v.value >> 2 << 2) << endl;
  cerr << (void*)(x.v.value ^ 0x02) << endl;
  cerr << (void*)(x.v.ptr.ptr << 2) << endl;
  cerr << x << endl;
}

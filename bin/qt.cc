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

  Var y( List::empty());


  size_t x = 1<<12;
  while (x--)
    y = y + Var((int)x);

  reference_counted::collect_cycles();

}

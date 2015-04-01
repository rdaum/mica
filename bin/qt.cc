#include <iostream>

#include "types/Data.hh"

using namespace mica;
using namespace std;

namespace mica {
extern void perform_compile();
}

int main(int argc, char *argv[]) {
  perform_compile();
}

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"


#include <sstream>
#include <algorithm>
#include "Scalar.hh"

#include "Exceptions.hh"

#include "Var.hh"

#include "AbstractBlock.hh"


using namespace mica;

bool AbstractBlock::isBlock() const
{
  return true;
}



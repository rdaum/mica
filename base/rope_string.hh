#ifndef MICA_ROPE_STRING
#define MICA_ROPE_STRING

#include "common/mica.h"
#include "config.h"


#include <string>

#ifdef HAVE_EXT_ROPE
#include <ext/rope>
#else
#include <rope>
#endif


namespace mica {
  
  typedef STD_EXT_NS::rope<char> mica_string;

}

#endif /** MICA_ROPE_STRING **/

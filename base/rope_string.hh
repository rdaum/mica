#ifndef MICA_ROPE_STRING
#define MICA_ROPE_STRING

#include "common/mica.h"
#include "config.h"


#ifdef HAVE_EXT_HASH_MAP
#include <ext/rope>
#else
#include <rope>
#endif


namespace mica {
  
  typedef STD_EXT_NS::rope<char> mica_string;
}

#endif /** MICA_ROPE_STRING **/

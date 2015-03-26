/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef COMPILE_HH
#define COMPILE_HH

#include "base/Ref.hh"

namespace mica {

/** Compile a string representing the source for a method into a method
 *  object
 */
class Block;
extern Ref<Block> compile(mica_string source);
}

#endif

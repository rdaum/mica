/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef COMPILE_HH
#define COMPILE_HH

namespace mica {

  /** Compile a string representing the source for a method into a method
   *  object
   */
  class Block;
  extern Ref<Block> compile( rope_string source );

}

#endif

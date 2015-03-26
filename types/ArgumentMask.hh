#ifndef MICA_ARGUMENT_MASK_HH
#define MICA_ARGUMENT_MASK_HH

#include <boost/numeric/conversion/cast.hpp>
#include <stdint.h>

namespace mica {

/** Just a simple bitmask set so we can tell when a method argument
  *  has been visited.  By using int, we are limiting ourselves to
  *  32 (on a 32-bit machine) possible arguments.  This doesn't
  *  seem unreasonable, however, does it?
  */
struct ArgumentMask {
  uint32_t marked_args;
  int dispatch_generation;

  ArgumentMask() : marked_args(0){};

  inline void clear() { marked_args = 0; }

  inline void mark_argument(unsigned int argument_no) { marked_args |= (1 << argument_no); }

  inline bool marked_argument(unsigned int argument_no) const {
    return bool(marked_args & (1 << argument_no));
  }

  inline bool marked_all_of(unsigned int arguments) const {
    return marked_args == boost::numeric_cast<uint32_t>((1 << arguments) - 1);
  }
};
}

#endif /** MICA_ARGUMENT_MASK_HH **/

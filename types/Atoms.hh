#ifndef MICA_TYPES_ATOMS_HH
#define MICA_TYPES_ATOMS_HH

namespace mica {

/** To identify non-numeric atom types
 */
struct Atoms {
  typedef enum { CHAR, SYMBOL, OPCODE, BOOLEAN, NONE } types;
};

struct _Atom {
  bool is_integer : 1;
  bool is_float : 1;
  bool is_pointer : 1;
  Atoms::types type : 3;
  signed int value ;
};

}  // namespace mica

#endif  // MICA_TYPES_ATOMS_HH


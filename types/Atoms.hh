#ifndef ATOMS_HH
#define ATOMS_HH

/** To identify non-numeric atom types
 */
struct Atoms {
  typedef enum { CHAR, SYMBOL, OPCODE, BOOLEAN } types;
};
struct _Atom {
  bool            is_integer : 1;
  bool            is_pointer : 1;
  Atoms::types    type       : 2;
  signed int      value      : 28;
};

#endif /** ATOMS_HH **/

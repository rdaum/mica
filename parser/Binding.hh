/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef BINDING_HH
#define BINDING_HH

namespace mica {

  class Var;

  /** A Binding provides a way of binding names to stack offsets in
   *  a block-nested fashion.  This is used at compile time to map variable
   *  names to stack pointers.  It is not kept around after compile.
   */
  class Binding {

  public:
    typedef var_vector BindMap;

    BindMap bindStack; 

    std::vector<unsigned int> lastBlockPos;

  public:
    void startBlock();

    unsigned int define( const Var &name );

    unsigned int lookup( const Var &name ) const;

    unsigned int finishBlock();

  };

}

#endif

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef NATIVEBLOCK_HH
#define NATIVEBLOCK_HH

#include "AbstractBlock.hh"

namespace mica {

  class Block;
  class NativeClosure;

  class NativeBlock
    : public AbstractBlock
  {
  public:
    Type::Identifier type_identifier() const { return Type::NATIVEBLOCK; }

  protected:
    friend class Unserializer;
    friend class NativeClosure;

    /** The actual function pointer
     */
    Var (*function)( const Ref<NativeClosure> &closure);

    /** We need these so we can serialize and unserialize the NativeBlock
     *  libraryName is the name of the DLL that holds the symbols.
     *  symbolName is the mangled name of the function inside the DLL.
     */
    rope_string libraryName;
    rope_string symbolName;

  public:
    NativeBlock( Var (*function)( const Ref<NativeClosure> &closure),
		 const rope_string &libraryName,
		 const rope_string &symbolName );

    virtual ~NativeBlock() {};

    Ref<Task> make_closure( const Ref<Message> &msg, const Var &definer );
   
    rope_string serialize() const;

    rope_string rep() const;

    rope_string tostring() const;

  };

}

#endif

/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef BLOCK_HH
#define BLOCK_HH

#include "AbstractBlock.hh"
#include "VariableStorage.hh"

namespace mica {
  
  class Closure;


  class Block
    : public AbstractBlock
  {
  public:
    Type::Identifier type_identifier() const { return Type::BLOCK; }

  public:
    var_vector code;   // The code storage of the block.   

    std::vector<int> statements;// Encoding of PC positions to statement
                                // #.
    std::vector<int> line_nos;  // Statement -> source line # mapping.
                             // is index in the stack.

    mica_string source;      // the source of the method.

    unsigned int add_scope;  // How many variables the block adds to scope


    
  public:
    Block( const mica_string &source );

    Block( const Ref<Block> &from );

  public:
    /** Add a line # for a program counter position
     */
    void add_line( int pc, int lineno );
    
    /** Return the line # for a program counter position
     */
    virtual int pc_to_line( int pc ) const;

  public:
    mica_string dump() const;

  public:
    Ref<Task> make_closure( const Ref<Message> &msg, const Var &definer );

  public:

    virtual mica_string serialize() const;

    virtual mica_string tostring() const;

    virtual mica_string rep() const;

  public:
    child_set child_pointers() ;
    
  protected:

    virtual mica_string serCommon( const mica_string &type ) const;

  };



}

#endif /* BLOCK_HH */


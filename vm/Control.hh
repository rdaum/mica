/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef MICA_EXECUTIONCONTEXT_H
#define MICA_EXECUTIONCONTEXT_H

#include "Var.hh"

namespace mica {

  class Block;

  /** An execution context encapsulates the current state of program flow.
   *  It contains the program counter and a pointer to the block.
   */
  class Control
  {
  public:
    /** Create an execution context for a block
     *  @param block to execute
     */
    Control( const Ref<Block> &block );

    /** Copy an execution context
     *  @param from EC to copy from
     */
    Control( const Control &from );

    /** Assign one control into the other
     */
    Control &operator=( const Control &from );

  public:
    void set_block( const Ref<Block> &block );

    /** Return the next opcode or value available from the current
     *  expression.
     */
    Var next_opcode();

    /** Clear exec_stack and reset program counter to beginning position
     */
    void reset();

    /** Query if the block is finished execution
     *  @return true or false
     */
    bool finished() const;

  public:
    /** Ask for the current line number
     */
    int current_line() const;

  public:
    void serialize_to( serialize_buffer &s_form ) const;


  public:
    void append_child_pointers( child_set &child_list );

    friend class Unserializer;

    int _pc;

    Ref<Block> block;

    var_vector exec_stack;
  };

}

#endif

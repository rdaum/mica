/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include <sstream>

#include "Data.hh"
#include "Exceptions.hh"
#include "Var.hh"
#include "Atom.hh"
#include "Task.hh"
#include "Block.hh"

#include "Frame.hh"
#include "Control.hh"

using namespace mica;
using namespace std;


mica_string Control::serialize() const {
  mica_string s_form;
  
  Pack( s_form, _pc );

  s_form.append( block->serialize() );
 
  return s_form;
}

child_set Control::child_pointers() {
  child_set child_p;
  child_p.push_back( (Block*)block );

  append_datas( child_p, exec_stack );

  return child_p;
}

Control::Control( const Ref<Block> &iBlock )
  : _pc(-1), block(iBlock)
{
  
}

Control::Control( const Control &from )
  : _pc(from._pc), block(from.block), exec_stack(from.exec_stack)
{}

Control &Control::operator=( const Control &from ) {
  if (&from != this) {
    _pc = from._pc;
    block = from.block;
    exec_stack = from.exec_stack;
  }
  return *this;
}

void Control::reset() {
  exec_stack.clear();
  _pc = -1;
}

Var Control::next_opcode()
{
  Var op;
  
  _pc++;
  
  if (_pc >= (int)block->code.size())
    throw internal_error("attempt to read beyond end of program");

  return block->code[_pc];  
}

int Control::current_line() const
{
  return block->pc_to_line( _pc );
}

void Control::set_block( const Ref<Block> &new_block ) 
{
  block = new_block;
}

bool Control::finished() const
{
  size_t c_size = block->code.size();
  return (_pc >= (int)c_size - 1 || !c_size );
}



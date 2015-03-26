/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "vm/Control.hh"

#include <sstream>

#include "common/mica.h"
#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/Var.hh"
#include "vm/Block.hh"
#include "vm/Frame.hh"
#include "vm/Task.hh"

using namespace mica;
using namespace std;

void Control::serialize_to(serialize_buffer &s_form) const {
  Pack(s_form, _pc);

  block->serialize_to(s_form);
}

void Control::append_child_pointers(child_set &child_list) {
  child_list.push_back((Block *)block);

  append_datas(child_list, exec_stack);
}

Control::Control(const Ref<Block> &iBlock) : _pc(-1), block(iBlock) {}

Control::Control(const Control &from)
    : _pc(from._pc), block(from.block), exec_stack(from.exec_stack) {}

Control &Control::operator=(const Control &from) {
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

Var Control::next_opcode() {
  Var op;

  _pc++;

  if (_pc >= (int)block->code.size())
    throw internal_error("attempt to read beyond end of program");

  return block->code[_pc];
}

int Control::current_line() const { return block->pc_to_line(_pc); }

void Control::set_block(const Ref<Block> &new_block) { block = new_block; }

bool Control::finished() const {
  size_t c_size = block->code.size();
  return (_pc >= (int)c_size - 1 || !c_size);
}

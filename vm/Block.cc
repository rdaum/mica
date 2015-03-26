/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "vm/Block.hh"

#include <algorithm>
#include <sstream>


#include "types/Data.hh"
#include "types/Exceptions.hh"
#include "types/Object.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "vm/Control.hh"
#include "vm/Frame.hh"
#include "vm/OpCodes.hh"
#include "vm/Scheduler.hh"

using namespace mica;

Block::Block(const mica_string &isource) : source(isource), add_scope(0) { code.clear(); }

Block::Block(const Ref<Block> &from)
    : code(from->code), source(from->source), add_scope(from->add_scope) {}

struct opcode_rep {
  std::ostream &out;
  opcode_rep(std::ostream &stream) : out(stream) {}
  template <typename T>
  void operator()(const T &val) const {
    out << val;
  }
  void operator()(const Symbol &sym) const { out << "#" << sym.tostring(); }
  void operator()(Data *data) const { out << data->rep(); }

  void operator()(const Op &opcode) const {
    if ((int)opcode.code >= 0)
      out << opcodes[opcode.code]->name;
    else
      out << "|";
  }
};

mica_string Block::dump() const {
  std::ostringstream out;
  opcode_rep printer(out);
  var_vector::const_iterator x;
  for (x = code.begin(); x != code.end(); x++) {
    x->apply_visitor<void>(printer);
    out << " ";
  }
  out << std::ends;
  return mica_string(out.str().c_str());
}

void Block::add_line(int num_opcodes, int lineno) {
  statements.push_back(num_opcodes);
  line_nos.push_back(lineno);
}

int Block::pc_to_line(int pc) const {
  /** Go through linenos, adding the relative pc positions there,
      until the passed in pc is greater than the added positions.
      The number of additions == the index for the #.
  */
  size_t idx = 0;
  size_t cnt = 0;
  if (statements.size()) {
    for (std::vector<int>::const_iterator x = statements.begin(); x != statements.end(); x++) {
      cnt += *x;
      if (pc <= (int)cnt)
        break;
      else
        idx++;
    }

    if (idx < line_nos.size()) {
      return line_nos[idx];
    }
  }
  return 0;
}

Ref<Task> Block::make_frame(const Ref<Message> &msg, const Var &definer) {
  /** mica blocks get a Frame.  We create a new one with all the
   *  right values copied from the message.
   */
  Ref<Frame> new_frame(new Frame(msg, definer, this));

  /** Return it for scheduling.
   */
  return Ref<Task>((Task *)new_frame);
}

void Block::serialize_to(serialize_buffer &s_form) const {
  Pack(s_form, type_identifier());

  /** Write opcodes
   */
  SerializeVV(s_form, code);

  /** Write source.
   */
  serialize_string(s_form, source);

  /** Write statement sizes
   */
  Pack(s_form, statements.size());
  std::vector<int>::const_iterator inti;
  for (inti = statements.begin(); inti != statements.end(); inti++) Pack(s_form, *inti);

  /** Write line #s
   */
  Pack(s_form, line_nos.size());
  for (inti = line_nos.begin(); inti != line_nos.end(); inti++) Pack(s_form, *inti);

  /** Write add_scope
   */
  Pack(s_form, add_scope);
}

mica_string Block::tostring() const { return source; }

mica_string Block::rep() const {
  std::ostringstream out;
  opcode_rep printer(out);
  out << "(";
  var_vector::const_iterator x;
  for (x = code.begin(); x != code.end(); x++) {
    x->apply_visitor<void>(printer);
    out << " ";
  }
  out << ")";

  out << std::ends;
  return mica_string(out.str().c_str());
}

void Block::append_child_pointers(child_set &child_list) {
  // append code?
}

bool Block::isBlock() const { return true; }

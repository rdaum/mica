/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"


#include <sstream>
#include <algorithm>


#include "Data.hh"
#include "Var.hh"
#include "Exceptions.hh"
#include "Object.hh"
#include "Symbol.hh"
#include "Frame.hh"
#include "Scheduler.hh"
#include "OpCodes.hh"
#include "Control.hh"

#include "Block.hh"

using namespace mica;

Block::Block( const mica_string &isource )
  : source(isource), add_scope(0)   
{
  code.clear();

}

Block::Block( const Ref<Block> &from )
  : code(from->code),
    source(from->source),
    add_scope(from->add_scope)
{}


struct opcode_rep {
  std::ostream &out;
  opcode_rep( std::ostream &stream ) 
    : out(stream) {}
  template<typename T>
  void operator()( const T &val ) const {
    out << val;
  }
  void operator()( const Symbol &sym ) const {
    out << "#" << sym.tostring();
  }
  void operator()( Data *data ) const {
    out << data->rep();
  }

  void operator()( const Op &opcode ) const {
    if ((int)opcode.code >= 0)
      out << opcodes[opcode.code]->name;
    else
      out << "|";
  }
};

mica_string Block::dump() const
{
  std::ostringstream out;
  opcode_rep printer( out );
  var_vector::const_iterator x;
  for (x = code.begin(); x != code.end(); x++) {
    x->apply_visitor<void>( printer );
    out << " ";
  }
  out << std::ends;
  return mica_string(out.str().c_str());

}


void Block::add_line( int num_opcodes, int lineno )
{
  statements.push_back(num_opcodes);
  line_nos.push_back( lineno );
}

int Block::pc_to_line( int pc ) const
{
  /** Go through linenos, adding the relative pc positions there,
      until the passed in pc is greater than the added positions.
      The number of additions == the index for the #.
  */
  size_t idx = 0;
  size_t cnt = 0;
  if (statements.size()) {
    for (std::vector<int>::const_iterator x = statements.begin();
	 x != statements.end(); x++) {
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



Ref<Task> Block::make_frame( const Ref<Message> &msg, const Var &definer )
{
  /** mica blocks get a Frame.  We create a new one with all the
   *  right values copied from the message.
   */
  Ref<Frame> new_frame(new (aligned) Frame( msg, definer, this ));

  /** Return it for scheduling.
   */
  return Ref<Task>((Task*)new_frame);
}

mica_string Block::serCommon( const mica_string &typen ) const
{
  mica_string s_form;

  Pack( s_form, type_identifier() );

  /** Write opcodes
   */
  size_t x = code.size();
  Pack( s_form, x );
  for (var_vector::const_iterator ni = code.begin(); ni != code.end(); ni++) {
    s_form.append( ni->serialize() );
  }

  /** Write source.
   */
  writeString( s_form, source );

  /** Write statement sizes
   */
  x = statements.size();
  Pack( s_form, x );
  std::vector<int>::const_iterator inti;
  for (inti = statements.begin(); inti != statements.end(); inti++) {
    int val = *inti;
    Pack( s_form, val );
  }

  /** Write line #s
   */
  x = line_nos.size();
  Pack( s_form, x );
  for (inti = line_nos.begin(); inti != line_nos.end(); inti++) {
    int val = *inti;
    Pack( s_form, val );
  }

  /** Write add_scope
   */
  Pack( s_form, add_scope );

  return s_form;
}

mica_string Block::serialize() const
{
  return serCommon("Block");
}

mica_string Block::tostring() const
{
  return source;
}

mica_string Block::rep() const
{
  std::ostringstream out;
  opcode_rep printer( out );
  out << "(";
  var_vector::const_iterator x;
  for (x = code.begin(); x != code.end(); x++) {
    x->apply_visitor<void>( printer );
    out << " ";
  }
  out << ")";

  out << std::ends;
  return mica_string(out.str().c_str());

}

void Block::append_child_pointers( child_set &child_list ) {
  // append code?
}

bool Block::isBlock() const { return true; }

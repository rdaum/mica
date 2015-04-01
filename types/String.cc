/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "types/String.hh"

#include <algorithm>
#include <boost/cast.hpp>
#include <utility>

#include "types/Exceptions.hh"
#include "types/List.hh"
#include "types/MetaObjects.hh"
#include "types/Var.hh"

namespace mica {

String::String() : Data(), mica_string() {
}

String::String(const String &from) : Data(), mica_string(from) {
}

String::String(const mica_string &from) : Data(), mica_string(from) {
}

String::String(const char *from) : Data(), mica_string(from) {
}

Ref<String> String::create(const char *from) {
  return new String(from);
}

/** Everything below here is public
 */
Var String::from_string(const mica_string &from) {
  if (from.empty())
    return empty();
  else {
    return new String(from);
  }
}

/** Everything below here is public
 */
Var String::from_cstr(const char *from) {
  if (!strlen(from) || !from)
    return empty();
  else {
    return new String(from);
  }
}

mica_string String::as_rope() const {
  return *this;
}

bool String::operator==(const Var &rhs) const {
  if (rhs.type_identifier() != type_identifier())
    return false;

  String *other = rhs->asType<String *>();

  return this == other || *this == *other;
}

bool String::operator<(const Var &rhs) const {
  if (rhs.type_identifier() != type_identifier())
    return false;

  return *this < *(rhs->asType<String *>());
}

Var String::add(const Var &v2) const {
  //  mica_string x = *this + v2;
  mica_string x(*this);

  if (v2.type_identifier() == Type::CHAR)
    x.push_back(v2.tochar());
  else
    x.append(v2.tostring());

  return new String(x);
}

Var String::sub(const Var &v2) const {
  throw unimplemented("sublist subtraction");
}

Var String::mul(const Var &v2) const {
  /** Multiplying empty lists is stupid
   */
  if (this->mica_string::empty())
    return String::empty();

  /** Make X copies of this.
   */
  int copies = v2.toint();

  mica_string k;

  while (copies--) {
    //    k = k + *this;
    k.insert(k.end(), begin(), end());
  }

  return new String(k);
}

Var String::div(const Var &v2) const {
  throw unimplemented("invalid operand for division");
}

Var String::cons(const Var &el) const {
  mica_string n_vec;
  n_vec.push_back(el.tochar());
  n_vec.insert(n_vec.end(), begin(), end());

  return String::from_string(n_vec);
}

Var String::snoc(const Var &el) const {
  return this->add(el);
}

Var String::append(const Var &seq) const {
  mica_string result(*this);
  mica_string to_append(seq.tostring());
  result.insert(result.end(), to_append.begin(), to_append.end());

  return String::from_string(result);
}

Var String::lview() const {
  if (null())
    return empty();

  mica_string viewl;
  viewl.push_back(*begin());

  mica_string viewr;
  if (size() > 1)
    viewr.insert(viewr.end(), (begin() + 1), end());

  var_vector res;
  res.push_back(String::from_string(viewl));
  res.push_back(String::from_string(viewr));

  return List::from_vector(res);
}

Var String::lhead() const {
  if (null())
    throw out_of_range("no head for empty sequence");

  return (Var(*begin()));
}

Var String::ltail() const {
  if (null())
    return empty();

  mica_string res(*this);
  res.pop_back();
  return String::from_string(res);
}

Var String::rview() const {
  if (null())
    return empty();

  mica_string viewl;
  viewl.push_back(back());

  mica_string viewr(*this);
  viewr.pop_back();

  var_vector res;
  res.push_back(String::from_string(viewl));
  res.push_back(String::from_string(viewr));

  return List::from_vector(res);
}

Var String::rhead() const {
  if (null())
    throw out_of_range("no head for empty sequence");

  return Var(back());
}

Var String::rtail() const {
  if (null())
    return empty();

  mica_string res(begin() + 1, end());
  return String::from_string(res);
}

bool String::null() const {
  return this->mica_string::empty();
}

int String::size() const {
  return boost::numeric_cast<int>(this->mica_string::size());
}

Var String::concat() const {
  throw invalid_type("concat operation meaningless for strings");
}

Var String::reverse() const {
  mica_string result(*this);
  std::reverse(result.begin(), result.end());
  return String::from_string(result);
}

Var String::take(int i) const {
  if (i > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (i < 0)
    return String::empty();
  else
    return String::from_string(mica_string(begin(), begin() + i));
}

Var String::drop(int i) const {
  if (i > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (i < 0)
    return String::empty();
  else
    return String::from_string(mica_string(begin() + i, end()));
}

Var String::splitAt(int i) const {
  if (i > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (i < 0)
    return String::empty();
  else {
    mica_string splitl(begin(), begin() + i);
    mica_string splitr(begin() + i, end());
    var_vector result;
    result.push_back(String::from_string(splitl));
    result.push_back(String::from_string(splitr));
    return List::from_vector(result);
  }
}

Var String::subseq(int start, int length) const {
  if (start + length > boost::numeric_cast<int>(size()))
    return Var(this);
  else if (start < 0)
    return String::empty();
  else if (length < 0)
    return String::from_string(mica_string(begin() + start, end()));
  else
    return String::from_string(mica_string(begin() + start, begin() + start + length));
}

bool String::inBounds(int i) const {
  return (i < size());
}

Var String::lookup(const Var &N) const {
  int i(N.toint());

  if (!inBounds(i))
    throw out_of_range("index out of range");

  return Var(this->at(i));
}

Var String::lookupM(int i) const {
  if (!inBounds(i))
    return NONE;
  else
    return Var(this->at(i));
}

Var String::lookup_withDefault(int i, const Var &d) const {
  if (!inBounds(i))
    return d;
  else
    return Var(this->at(i));
}

Var String::update(int i, const Var &e) const {
  if (!inBounds(i))
    return Var(this);
  else {
    mica_string result = *this;
    result[i] = e.tochar();
    return String::from_string(result);
  }
}

Var String::zip(const Var &with) const {
  throw invalid_type("invalid operands");
}

Var String::zipTriple(const Var &two, const Var &three) const {
  throw invalid_type("invalid operands");
}

Var String::unzip() const {
  throw invalid_type("invalid operands");
}

Var String::unzipTriple() const {
  throw invalid_type("invalid operands");
}

var_vector String::flatten() const {
  throw invalid_type("cannot flatten string type");
}

var_vector String::map(const Var &expr) const {
  /** Finished iterating.  No-op
   */
  if (this->mica_string::empty())
    return var_vector();

  /** Push cdr then push the rest of the expr
   */
  var_vector ops;
  ops.push_back(Var(Op::EVAL));
  ops.push_back(expr);
  ops.push_back(lhead());  // cdr

  /** recurse on car
   */
  if (size() > 1) {
    ops.push_back(Var(Op::MAP));

    ops.push_back(expr);

    /** car
     */
    ops.push_back(ltail());
  }

  return ops;
}

mica_string String::tostring() const {
  return *this;;
}

mica_string String::rep() const {
  mica_string x = "\"";

  x.append(*this);

  x.append("\"");

  return x;
}

void String::serialize_to(serialize_buffer &s_form) const {
  Pack(s_form, type_identifier());

  size_t len = size();

  Pack(s_form, len);

  s_form.append(*this);
}

size_t String::hash() const {
  std::hash <mica_string> hasher;
  return hasher(*this);
}

int String::toint() const {
  throw invalid_type("toint() called on collection");
}

float String::tofloat() const {
  throw invalid_type("tofloat() called on collection");
}

Var String::mod(const Var &v2) const {
  throw invalid_type("invalid operands");
}

Var String::neg() const {
  throw invalid_type("invalid operand");
}

}  // namespace mica

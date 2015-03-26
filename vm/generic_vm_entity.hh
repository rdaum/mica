#ifndef GENERIC_VM_ENTITY
#define GENERIC_VM_ENTITY

#include "types/Exceptions.hh"

namespace mica {

/** VM entities (tasks, lambdas, etc.) are typically scalars with similar
 *  characteristics.  This allows us to flattenolidate most of their behaviours
 *  so we don't have lots of replicated code.
 */
class generic_vm_entity : public Atom {
 public:
  virtual bool operator==(const Var &rhs) const {
    if (type_identifier() != type_identifier())
      return false;

    generic_vm_entity *x = rhs->asType<generic_vm_entity *>();

    return this == x;
  }

  virtual bool operator<(const Var &v2) const { return 0; }

  virtual Var add(const Var &rhs) const { throw invalid_type("invalid operands"); }

  virtual Var div(const Var &rhs) const { throw invalid_type("invalid operands"); }

  virtual Var mul(const Var &rhs) const { throw invalid_type("invalid operands"); }

  virtual Var sub(const Var &rhs) const { throw invalid_type("invalid operands"); }

  virtual Var mod(const Var &rhs) const { throw invalid_type("invalid operands"); }

  virtual Var pow(const Var &rhs) const { throw invalid_type("invalid operands"); }

  virtual Var neg() const { throw invalid_type("invalid operands"); }

  virtual Var inc() const { throw invalid_type("invalid operands"); }

  virtual Var dec() const { throw invalid_type("invalid operands"); }

  virtual int toint() const { throw invalid_type("invalid type conversion"); }

  virtual float tofloat() const { throw invalid_type("invalid type conversion"); }

  virtual bool isNumeric() const { return false; }

  virtual bool truth() const { return true; };

  virtual mica_string tostring() const { throw invalid_type("cannot convert to string"); }
};
}

#endif /** GENERIC_VM_ENTITY **/

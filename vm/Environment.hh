/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef MICA_ENVIRONMENT_HH
#define MICA_ENVIRONMENT_HH

namespace mica {

  class Var;
  typedef unsigned int VarIndex;
  using std::vector;

  class GCVector 
    : public reference_counted,
      public var_vector
  {
  public:
    GCVector()
      : reference_counted(), var_vector() {};

    GCVector( const GCVector &from )
      : var_vector(from) {}

    child_set child_pointers() {
      return data_list(*this);
    }
  };

  class Environment
  {
  protected:
    friend class Frame;

    Ref<GCVector> env;

    std::vector<unsigned int> widths;

  public:
    /** Create blank variable storage record.
     */
    Environment();

    /** Copy/inherit from another variable storage record
     *  @param from Variable storage record to copy from. 
     */
    Environment( const Environment &from );

    ~Environment();

  public:
    Environment copy() const;

    void enter( unsigned int W );

    void exit();

    /** Set the value of a local variable
     *  @param var VarIndex index of the variable.
     *  @param value new value of the variable
     */
    void set( unsigned int var, const Var &value );

    /** Retrieve the value of a local variable
     *  @param var VarIndex index of the variable.
     */
    Var get( unsigned int var );

  public:
    mica_string serialize() const;

    child_set child_pointers();

  };

}

#endif

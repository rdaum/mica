#ifndef MICA_SLOTS_HH
#define MICA_SLOTS_HH

namespace mica {

  /** Utilities to support proper slot declaration and matching.
   *  Handles delegation, proper names, multiple dispatch, etc.
   */
  struct Slots {
    
    static Slot get_slot( const Var &self,
			  const Var &accessor, 
			  const Symbol &name );
    
    static Slot get_name( const Var &self,
				const Symbol &name );
    
    static Slot get_delegate( const Var &self,
				    const Symbol &name );

    static Slot match_verb( const Var &self,
				  const Symbol &name,
				  const var_vector &arguments );
  
    static Slot get_verb( const Var &self,
				const Symbol &name,
				const var_vector &arg_template );


    static Var declare_verb( Var &self,
			     const Symbol &name,
			     const var_vector &argument_template,
			     const Var &method );

    static Var assign_verb( Var &self,
			    const Symbol &name,
			    const var_vector &argument_template,
			    const Var &method );

    static void remove_verb( Var &self,
			     const Symbol &name,
			     const var_vector &argument_template );

    /** Queries for an ancestor in the inheritance graph
     *  @param self object to do the inheritance graph search from
     *  @param ancestor object to search for in ineritance graph
     *  @return true if object is a relative
     */
    static bool isA( const Var &self, const Var &ancestor );
  };
}

#endif /** MICA_SLOTS_HH **/

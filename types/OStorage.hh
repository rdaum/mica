#ifndef MICA_ENVIRONMENT_HH
#define MICA_ENVIRONMENT_HH

#include "common/mica.h"
#include "config.h"

#include <map>

#ifdef HAVE_EXT_HASH_MAP
#include <ext/hash_map>
#else
#include <hash_map>
#endif

#include "Symbol.hh"

#include "hash.hh"

namespace mica {

  class Object;

  /** Each slot is defined by its name, accessor, and value
   */
  struct SlotEntry {
    Symbol name;
    Var accessor;
    Var value;

    SlotEntry( const Symbol &i_name, const Var &i_accessor, 
	       const Var &i_value )
      : name(i_name), accessor(i_accessor), value(i_value)
    {}
    
  };

  #define END_OF_ARGS_MARKER 65535
  class VerbDef:
    public reference_counted {
  public:
    Var definer;
    var_vector argument_template;
    Var method;

    child_set child_pointers();

    VerbDef();
    VerbDef( const VerbDef &x );
    virtual ~VerbDef() {};

    bool operator==( const VerbDef &x );
    VerbDef &operator=( const VerbDef &x );
    
  };
  typedef std::vector< Ref<VerbDef> > VerbList;

  class Environment
  {

  public:
    Environment();

    ~Environment();

  public:
    SlotEntry *getLocal( const Var &accessor, 
			 const Symbol &name ) const;

    SlotEntry *addLocal( const Var &accessor,
			 const Symbol &name, const Var &value );

    bool removeLocal( const Var &accessor, const Symbol &name );

    rope_string serialize() const;

    Var slots() const;

    child_set child_pointers();

  public:

    /** Carries a list of slots hashed by name
     */
    typedef STD_EXT_NS::hash_map< Symbol,
 				  SlotEntry*,
 				  hash_symbol > SlotList;


    /** Map accessor -> slotlist
     */
    typedef STD_EXT_NS::hash_map< Var, SlotList,
 				  hash_var > SlotMap;

    SlotMap mSlots;    


  public:
    typedef std::map< var_vector, Ref<VerbDef> > VerbTemplatesMap;

    typedef std::map< std::pair< Symbol, unsigned int >,
		      VerbTemplatesMap > VerbParasiteMap;

    VerbParasiteMap verb_parasites;


  public:

    void set_verb_parasite( const Symbol &name,
			    unsigned int pos,
			    const var_vector &argument_template,
			    const Var &definer,
			    const Var &method ) ;

    void rm_verb_parasite( const Symbol &name,
			   unsigned int pos,
			   const var_vector &argument_template ) ;
    
    VerbList get_verb_parasite( const Symbol &name,
				unsigned int pos ) const;


  protected:
    friend class Object;
    friend class PersistentPool;

    /** Cached parents_slot entry
     */
    SlotMap::iterator delegates_iterator;

    /** Define a delegate.
     */
    void add_delegate( const Object *from, 
		       const Symbol &name,
		       const Var &delegate );

    var_vector delegates() ;
  };

}

#endif /** MICA_ENVIRONMENT_HH **/


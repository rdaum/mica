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
  typedef var_vector VerbArgTemplate;

  #define END_OF_ARGS_MARKER 65535
  class VerbDef:
    public reference_counted {
  public:
    Var definer;
    VerbArgTemplate argument_template;
    Var method;

    child_set child_pointers();

    VerbDef();
    VerbDef( const VerbDef &x );
    virtual ~VerbDef() {};

    bool operator==( const VerbDef &x ) const;
    bool operator<( const VerbDef &x ) const;

    VerbDef &operator=( const VerbDef &x );
    
  };
  typedef std::vector< Ref<VerbDef> > VerbList;

  struct hash_verb_pair {
    unsigned int operator()( const std::pair< Symbol, 
			     unsigned int > &p ) const;
  };

  class OStorage
  {

  public:
    OStorage();

    ~OStorage();

  public:
    std::pair<bool, Var> getLocal( const Var &accessor, 
				   const Symbol &name ) const;

    bool addLocal( const Var &accessor,
		   const Symbol &name, const Var &value );

    bool replaceLocal( const Var &accessor, const Symbol &name,
		       const Var &value );

    bool removeLocal( const Var &accessor, const Symbol &name );


    mica_string serialize() const;

    Var slots() const;

    child_set child_pointers();

  public:

    /** Carries a list of slots hashed by name
     */
    typedef STD_EXT_NS::hash_map< Symbol,
				  Var, hash_symbol > SlotList;


    /** Map accessor -> slotlist
     */
    typedef STD_EXT_NS::hash_map< Var, SlotList, hash_var > SlotMap;

    SlotMap mSlots;    


  public:
    typedef std::list< Ref<VerbDef> > VerbTemplatesMap;

    typedef STD_EXT_NS::hash_map< std::pair< Symbol, unsigned int >,
				  VerbTemplatesMap,
				  hash_verb_pair > VerbParasiteMap;

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
    
    VerbList get_verb_parasites( const Symbol &name,
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


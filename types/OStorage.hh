#ifndef MICA_OSTORAGE_HH
#define MICA_OSTORAGE_HH

#include "common/mica.h"
#include "config.h"

#include <boost/pool/pool_alloc.hpp>

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

    void append_child_pointers( child_set &child_list );

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
    OptVar getLocal( const Var &accessor, 
		     const Symbol &name ) const;

    bool addLocal( const Var &accessor,
		   const Symbol &name, const Var &value );

    bool replaceLocal( const Var &accessor, const Symbol &name,
		       const Var &value );

    bool removeLocal( const Var &accessor, const Symbol &name );


    void serialize_to( serialize_buffer &s_form ) const;

    Var slots() const;

    void append_child_pointers( child_set &child_list );

  public:

    /** Carries a list of slots hashed by name
     */
    typedef STD_EXT_NS::hash_map< Symbol,
				  Var, hash_symbol,
				  std::equal_to<Symbol>,
				  boost::pool_allocator<Symbol> > OptSlotList;


    /** Map accessor -> slotlist
     */
    typedef STD_EXT_NS::hash_map< Var, OptSlotList, hash_var,
				  std::equal_to<Var>,
				  boost::pool_allocator<Var> > OptSlotMap;

    OptSlotMap mOptSlots;    


  public:
    typedef STD_EXT_NS::hash_multimap< std::pair< Symbol, unsigned int >,
				       Ref<VerbDef>,
				       hash_verb_pair,
				       std::equal_to<std::pair< Symbol, unsigned int > >,
				       boost::pool_allocator< std::pair< Symbol, unsigned int > > > VerbParasiteMap;

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

    /** Cached delegates slot entry
     */
    OptSlotMap::iterator delegates_iterator;

    /** Define a delegate.
     */
    void add_delegate( const Object *from, 
		       const Symbol &name,
		       const Var &delegate );

    var_vector delegates() ;
  };

}

#endif /** MICA_ENVIRONMENT_HH **/


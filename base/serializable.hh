#ifndef MICA_SERIALIZABLE_HH
#define MICA_SERIALIZABLE_HH

#include "config.h"

#ifdef HAVE_EXT_ROPE
#include <ext/rope>
#else
#include <rope>
#endif


#include "rope_string.hh"

namespace mica {

  typedef STD_EXT_NS::rope<char> serialize_buffer;

  class serializable {
  public:
    virtual void serialize_to( serialize_buffer &s_form ) const = 0;
  };

  /** For serializing the contents of simple types
   *  This append the literal binary form of the type to
   *  the passed-in string.
   */
  template<class T>
  inline void Pack( serialize_buffer &S, const T &N ) {
    S.append( (char*)&N, sizeof(T) );
  }

  inline void serialize_string( serialize_buffer &s_form, 
				const mica_string &istr )
  {
    size_t len = istr.size();
    Pack( s_form, len );
    s_form.append( istr );
  }

};

#endif /** MICA_SERIALIZABLE **/

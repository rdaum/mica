/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef HASH_HH
#define HASH_HH

/** Function objects for using in hashing
 */

namespace mica {

  template<class T>
  struct ptr_hash
  {
    static const unsigned int bucket_size = 4;
    static const unsigned int min_buckets = 16;

    unsigned int operator()( const T *ptr ) const {
      return (size_t)ptr + 2743 + 5923;
    }
    
  };
  
  /** hash two vars by using the .hash() method protocol
   */
  struct hash_var
  {
    static const unsigned int bucket_size = 4;
    static const unsigned int min_buckets = 16;

    unsigned int operator()( const Var &var ) const;
  };

  struct hash_long_long 
  {
    static const unsigned int bucket_size = 4;
    static const unsigned int min_buckets = 16;

    inline unsigned int operator()( unsigned int long long x ) const
    {
      return x & 0x7fffffff;
    }
  };

  /** hash two refs by using the .hash() method
   */
  struct hash_ref
  {
    static const unsigned int bucket_size = 4;
    static const unsigned int min_buckets = 16;

    template<class T>
    unsigned int operator()( const Ref<T> &ref ) const {
      return ref.hash(); 
    }
  };


  /** hashes on a pair
   */
  struct pair_hash
  {
    static const unsigned int bucket_size = 4;
    static const unsigned int min_buckets = 16;

    template<class T, class X>
    unsigned int operator()( const std::pair<T, X> &key ) const {
      return (key.first.hash() << 16) + key.second.hash();
    }
  };

  /** hashes on rope_string
   */
  struct str_hash
  {
    static const unsigned int bucket_size = 4;
    static const unsigned int min_buckets = 16;

    unsigned int operator()( const rope_string &str ) const;
  };

  class Symbol;
  struct hash_symbol {
    unsigned int operator()( const Symbol &sym ) const;
  };

}
#endif

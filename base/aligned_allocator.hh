#ifndef MMAP_ALLOC
#define MMAP_ALLOC

#include <memory>
#include <iostream>
#include <stdlib.h>

inline void *mica_memalign( size_t align, size_t size, void **ptr ) {
#ifdef HAVE_POSIX_MEMALIGN
  return ( !posix_memalign( ptr, align, size ) ? *(pp_orig) : NULL );
#elif defined( HAVE_MEMALIGN )
  return ( *(ptr) = memalign( align, size ) );
#else /* We don't have any choice but to align manually */
  (( *(ptr) = malloc( size + align - 1 )) \
   ? (void *)( (((unsigned long)*(ptr)) + (unsigned long)(align-1) ) \
	       & (~(unsigned long)(align-1)) ) \
   : NULL );
#endif
    }
namespace mica { 


  inline void* do_alloc( size_t size_of ) {

    /** This is _supposed_ to be aligned on 8-byte blocks
     *  which is good, because we use some lower bits for
     *  storing goodies
     */
    void *addr = 0;
    mica_memalign( 8, size_of, &addr );
    if (addr == 0) {
      throw std::bad_alloc();
    }
    return addr;
  }

  /** An STL-compatible allocator that is used to allocate 
   *  memory from the heap only in 4-byte aligned blocks.
   *  This is probably the default behaviour anyways, but
   *  we use this allocator just to be sure.
   */
  template <typename T> class aligned_allocator
  {
  public:
    typedef T                 value_type;
    typedef value_type*       pointer;
    typedef const value_type* const_pointer;
    typedef value_type&       reference;
    typedef const value_type& const_reference;
    typedef std::size_t       size_type;
    typedef std::ptrdiff_t    difference_type;

  public:
    aligned_allocator() {}

    aligned_allocator(const aligned_allocator&) {}

    ~aligned_allocator() {}

  private:

    void operator=(const aligned_allocator&);

  public: 
    /** Allocates storage for num elements of type T.
     *  @return pointer to the newly allocated block
     */
    pointer allocate (size_type num, void* = 0)  { 
      return (pointer) do_alloc( sizeof(T) * num );
    } 


    /** Frees storage of a previously allocated block.
     */
    void deallocate (pointer p, size_type) { 
      free( (void*) p );
    }   

    /** Can be used to call the constructor on a type
     */
    void construct(pointer p, const value_type& x) { 
      new(p) value_type(x); 
    }

    /** Can be used to call the destructor on a type
     */
    void destroy(pointer p) { 
      p->~value_type(); 
    }

  
    template <class U> 
    aligned_allocator(const aligned_allocator<U>&) {}

    template <class U> 
    struct rebind { 
      typedef aligned_allocator<U> other; 
    };

  }; 

  template <class T>
  inline bool operator==(const aligned_allocator<T>&, 
			 const aligned_allocator<T>&) {
    return true;
  }

  template <class T>
  inline bool operator!=(const aligned_allocator<T>&, 
			 const aligned_allocator<T>&) {
    return false;
  }


  struct Aligned { };
  extern Aligned aligned;

 
}

/** Traditional malloc-based new.
 */
inline void *operator new( size_t size ) 
  throw (std::bad_alloc)
{
  void *addr = malloc( size );
  if (addr == 0) {
    throw std::bad_alloc();
  }
  return addr;
}


/** Replacement new operator which can be used for allocating
 *  on aligned blocks.
 */
inline void *operator new( size_t size, mica::Aligned &a ) 
  throw (std::bad_alloc)
{
  return mica::do_alloc( size );
}

#endif 

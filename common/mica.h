/* Placeholder header file.  
 * Note that it's Mica.h, not Mica.hh -- it should look like a C header to C
 */

#if !defined(POE_H)
#define POE_H

#define STD_EXT_NS std

/**** GCC-specific damage ****/

#if defined (__GNUG__)

# if __GNUC__ == 2
/** It appears that, in GCC 2.95.4 at least, ostringstream
 *  will append and leave \0 in the character stream that
 *  it returns via .str().
 */
#define OSTRSTREAM_APPENDS_NULLS 
#endif

/** Versions of libstdc++-3 for GCC 3.1 and above put ext/ into
 *  __gnu_cxx.  Other STLs put them in std:: along with everything
 *  else.  Who is right? -- ryan
 */
#  if __GNUC__ == 3 && __GNUC_MINOR__ > 0
#    undef STD_EXT_NS
#    define STD_EXT_NS __gnu_cxx
#  endif
#endif


/**** MSVC Specific damage ****/

/* Disable some of the dumber warnings in VC++ */
#ifdef _MSC_VER
#  pragma warning(disable:4786) /* symbol truncated in debug info */
#  pragma warning(disable:4800) /* implicit bool casts */
#  pragma warning(disable:4514) /* unreferenced inline */

#  if _MSC_VER <= 1200 /* Visual C++ 6.0 (using the word C++ loosely) */
#    define MSVC6
#    define BROKEN_TEMPLATE_FUNCTIONS
#  endif
#endif

/**** Windows Specific, general ****/

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  define VC_EXTRALEAN
#endif

#endif /* POE_H */


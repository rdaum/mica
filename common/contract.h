#ifndef POE_CONTRACT_H
#define POE_CONTRACT_H

#define EIFFEL_CHECK CHECK_INVARIANT

#ifdef HAVE_NANA_H

#define     ASSERT I
#define ASSERT_NOT N

#define TRUE_FOR_ALL AO
#define TRUE_FOR_ANY EO
#define TRUE_FOR_ONE E10

#define  PRECONDITION REQUIRE
#define POSTCONDITION ENSURE

#else
// let this be a C header too, so no <cassert>
#include "assert.h"

#define     ASSERT(expr) assert(expr)
#define ASSERT_NOT(expr) assert(!(expr))

/* these will not check invariants */
#define  PRECONDITION assert
#define POSTCONDITION assert


/* I could define these, but I don't feel like it ATM */
#define TRUE_FOR_ALL(name,container,predicate)
#define TRUE_FOR_ANY(name,container,predicate)
#define TRUE_FOR_ONE(name,container,predicate)


/* all of nana's macros except I() won't work */

#define I(expr) assert(expr)
#define ID(expr)
#define IS(expr)
#define ISG(expr)

#define DI(expr)
#define DS(expr)
#define DSG(expr)
#define GDB(expr)

#define CO(name,container,expr)
#define SO(name,container,expr)
#define PO(name,container,expr)

#endif

#endif

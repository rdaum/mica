# Process this file with autoconf to produce a configure script.
AC_INIT(mico, 0.1, mico-bugs@mico.sf.net)
AC_CONFIG_SRCDIR([types/Var.cc])
AC_CONFIG_HEADER([config.h])

AC_LANG(C++)

# Checks for programs.
AC_PROG_CXX
AC_PROG_RANLIB
AC_HEADER_STDC
AC_CHECK_FUNCS(memalign posix_memalign)

################
dnl Check for system libs needed
AC_CHECK_FUNCS(select getpwuid)


# save the old value of CXXFLAGS since we want to break out INCLUDES
OLD_CXXFLAGS=$CXXFLAGS

                         

################

AC_CHECK_LIB(dl,dlopen, [LIBS="$LIBS -ldl"])
AC_CHECK_LIB(nana, nana_error, [LIBS="$LIBS -lnana"])

################
# Checks for header files.
AC_HEADER_STDC

AC_CHECK_HEADER(nana.h)
AC_CHECK_HEADERS([hash_map ext/hash_map map], break)
AC_CHECK_HEADERS([hash_set ext/hash_set set], break)
AC_CHECK_HEADERS([rope ext/rope], break)


################
# --with-bdb=
AC_ARG_WITH(bdb,
  AC_HELP_STRING([--with-bdb=location], 
                 [location to find Berkeley DB installation]),
  [BDB_HOME=$with_bdb])

OLDLIBS=$LIBS
LIBS="-ldb_cxx"

if [test -n "$BDB_HOME" ]; then
    # for the benefit of AC_SUBST
    INCLUDES="$INCLUDES -I${BDB_HOME}/include"
    LDFLAGS="$LDFLAGS -L${BDB_HOME}/lib"

    # for the benefit of AC_TRY_LINK only
    CXXFLAGS="$CXXFLAGS -I${BDB_HOME}/include"
fi

echo -n "checking whether berkeley db works ... "
# note that this currently only tests for the C++ API
AC_TRY_LINK([#include <db_cxx.h>],
	    [Db *foo; foo = new Db(NULL,0);], 
	    [BDB_WORKS=true])

if test -n "$BDB_WORKS"; then
  echo "yes"
  echo -n "checking whether Db->open requires an extra argument ... "
  AC_TRY_LINK([#include <db_cxx.h>],
   	    [Db *dbp; dbp = new Db(NULL,0); int ret = dbp->open(NULL, "autoconftest", NULL, DB_BTREE, 0, 0);], 
    	    [DB_OPEN_REQUIRES_EXTRA_ARG=true])

  AH_TEMPLATE(DB_OPEN_REQUIRES_EXTRA_ARG,
	      [In newer versions of Berkeley/Sleepycat DB, the Db::open call
	       requires a new first argument, which can be safely set to NULL.
	       But many machines are still using older versions, so this var
	       is set only if needed.])
  if test -n "$DB_OPEN_REQUIRES_EXTRA_ARG"; then
    echo "yes"
    AC_DEFINE(DB_OPEN_REQUIRES_EXTRA_ARG)
  else
    echo "no"
  fi

  LIBS="$OLDLIBS"
  LIBS="$LIBS -ldb_cxx"
else
  LIBS="$OLDLIBS"
  echo "no"    
fi

test "x$GXX" = "xyes" && LDFLAGS="$LDFLAGS -Wl,-export-dynamic"

################
# --enable-dso-native
AC_ARG_ENABLE(dso-builtins,
  AC_HELP_STRING([--enable-dso-builtins], 
                 [put bootstrap methods in a DLL (EXPERIMENTAL)]))

if test "x$enableval" = xyes; then
    OPT_ENABLE_DSO_BUILTINS=true
else
    OPT_ENABLE_DSO_BUILTINS=false
fi



# Checks for typedefs, structures, and compiler characteristics.
dnl AC_C_CONST
AC_C_INLINE
dnl AC_TYPE_OFF_T
dnl AC_TYPE_SIZE_T
dnl AC_HEADER_TIME

CXXFLAGS=$OLD_CXXFLAGS

AC_SUBST(AR,ar)
AC_SUBST(INCLUDES)
AC_SUBST(LIBS)
AC_SUBST(LDFLAGS)

AC_SUBST(OPT_ENABLE_DSO_BUILTINS)

AC_CONFIG_FILES([mk/defs.mk])
AC_OUTPUT

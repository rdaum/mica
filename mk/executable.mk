# -*- Makefile -*-

ifndef EXECUTABLE_MK_INCLUDE_GUARD
EXECUTABLE_MK_INCLUDE_GUARD = true

include ../mk/defs.mk

ifdef RONG

ifdef TARGET
ifndef TARGETS
TARGETS=$(TARGET)
endif
endif

TARGETS ?= $(error TARGET or TARGETS must be defined before including executable.mk)

endif

STATICS =  ../parser/mica_parser.a  ../persistence/mica_persistence.a   ../native/mica_nativebind.a ../vm/mica_vm.a ../types/mica_types.a ../base/mica_base.a 

ifeq "$(OPT_ENABLE_DSO_BUILTINS)" "true"
NATIVELIB = "-llibmicanativevar"
endif

# the order of these is very specific.  yes they are repeated.  
# do not rearrange them or Ryan will spank you.
EXE_LDARGS = ../parser/mica_parser.a   ../persistence/mica_persistence.a   ../native/mica_nativebind.a ../vm/mica_vm.a ../types/mica_types.a $(NATIVELIB) ../persistence/mica_persistence.a ../native/mica_nativebind.a ../parser/mica_parser.a ../vm/mica_vm.a ../base/mica_base.a  ../types/mica_types.a -llog4cpp 

include ../mk/common.mk

endif

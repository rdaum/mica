# -*- Makefile -*-

ifndef STATICLIB_MK_INCLUDE_GUARD
STATICLIB_MK_INCLUDE_GUARD = true

include ../mk/defs.mk

TARGET ?= $(error TARGET must be defined before including staticlib.mk)

SRCS ?= $(wildcard *.cc)
OBJS ?= $(patsubst %.cc,%.o, $(SRCS)) 

$(TARGET): $(OBJS) 
	-rm -f $(TARGET)
	$(AR) cru $(TARGET) $(OBJS) $(ADD_OBJS)
	$(RANLIB) $(TARGET)

include ../mk/common.mk

endif
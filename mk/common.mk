# -*- Makefile -*-

ifndef COMMON_MK_INCLUDE_GUARD
COMMON_MK_INCLUDE_GUARD = true

include ../mk/defs.mk

.SUFFIXES: .cc

.cc.o:
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(DEFINES) -c $< -o $@


-include ./depend.mk

# make depend is totally dependent on gcc behavior, so gcc is hardwired
depend:
	-gcc -MM $(INCLUDES) *.cc > ./depend.mk

clean:
	rm -f *.o *.so *.a *~ $(CLEAN)

.PHONY: clean depend

endif
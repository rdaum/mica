INCLUDES += -I../types -I../vm -I../xml -I../native -I../persistence -I../parser
LIBS += -lcppunit

all: mica_test mica_test_vm test_refcnt # test_slotstore
include ../mk/executable.mk
CXXFLAGS += -ftemplate-depth-64

mica_test: main.o $(STATICS)
	$(CXX) $< $(LDFLAGS) $(EXE_LDARGS) $(LIBS) -o $@ 

mica_test_vm: vm_test.o $(STATICS)
	$(CXX) $< $(LDFLAGS) $(EXE_LDARGS) $(LIBS) -o $@ 

test_refcnt: test_refcnt.o $(STATICS) ../parser/mica_parser.a
	$(CXX) $< $(LDFLAGS)  ../parser/mica_parser.a $(EXE_LDARGS)  $(LIBS)  -o $@ 

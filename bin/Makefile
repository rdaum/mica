INCLUDES += -I../types  -I../vm -I../native -I../persistence -I../network -I../parser

all: mica_console qt mica_daemon mica_makedb

include ../mk/executable.mk

compile.o: compile.cc ../parser/mica_parser.a

mica_makedb: micamakedb.o compile.o $(STATICS)
	$(CXX) micamakedb.o compile.o $(LDFLAGS) $(EXE_LDARGS) $(LIBS) -o $@

mica_console: micaconsole.o compile.o $(STATICS)
	$(CXX) micaconsole.o compile.o $(LDFLAGS)   $(EXE_LDARGS) $(LIBS) -lreadline -o $@ 

mica_daemon: micadaemon.o $(STATICS) 
	$(CXX) micadaemon.o $(LDFLAGS)   $(EXE_LDARGS) $(LIBS) -o $@ 

qt: qt.o $(STATICS) 
	$(CXX) qt.o $(LDFLAGS)  $(EXE_LDARGS) $(LIBS) -o $@ 

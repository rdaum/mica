TARGET=mica_parser.a
INCLUDES += -I../types -I../vm -I../base 
WHALE=whale
ADD_OBJS=parser.o lexer.o
DOLPHIN=dolphin

include ../mk/staticlib.mk
CLEAN = lexer.cpp lexer.h parser.cpp  parser.h \
        lexer.o parser.o ParseDriver.o parser.conflicts \
	test

lexer.o: lexer.cpp lexer.h parser.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -O2 -c lexer.cpp -o lexer.o

lexer.cpp lexer.h: lexer.dlp
	$(DOLPHIN) lexer.dlp

parser.cpp parser.h: parser.whl
	$(WHALE) parser.whl

parser.o: parser.cpp parser.h lexer.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -DWHALE_DEBUG=1 parser.cpp \
	  -o parser.o

MicaParser.o: MicaParser.cc MicaParser.hh parser.h lexer.h lexer.o parser.o 
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c MicaParser.cc -o MicaParser.o


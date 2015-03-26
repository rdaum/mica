CONFIG ?= Debug

PREFIX =

compile = \
	AR=$(PREFIX)ar \
	AS=$(PREFIX)as \
	CC=$(PREFIX)gcc \
	CXX=$(PREFIX)g++ \
	gyp $(1) --depth=. -f ninja && \
	ninja -v -C out/$(CONFIG)

cmake = \
	AR=$(PREFIX)ar \
	AS=$(PREFIX)as \
	CC=$(PREFIX)gcc \
	CXX=$(PREFIX)g++ \
	gyp $(1) --depth=. -f cmake 

.PHONY: all

all: mica

clean:
	ninja -v -C out/Debug -t clean
	ninja -v -C out/Release -t clean

nuke: 
	rm -rf out build

# Targets

cmakefile:
	$(call cmake, mica.gyp)

mica:
	$(call compile, mica.gyp)



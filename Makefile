include mk/defs.mk

DIRS = base types vm  parser persistence native  bin
DIRSDEPEND = $(patsubst %,%-depend,$(DIRS))
DIRSCLEAN = $(patsubst %,%-clean,$(DIRS))

all: fixup-pre $(DIRS) fixup-post

fixup-pre: ;

fixup-post: ;

$(DIRS):
	cd $@ && make

$(DIRSDEPEND):
	cd $(subst -depend,,$@) && make depend

$(DIRSCLEAN):
	cd $(subst -clean,,$@) && make clean

depend: $(DIRSDEPEND)

clean: $(DIRSCLEAN)

.SILENT: fixup-pre fixup-post
.PHONY: fixup-pre fixup-post all $(DIRS) $(DIRSDEPEND) $(DIRSCLEAN) clean quick-clean

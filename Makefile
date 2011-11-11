NAME = parmap
OBJS = bytearray $(NAME) version
INTF = $(foreach obj, $(OBJS),$(obj).cmi) setcore.cmi
C_OBJS = bytearray_stubs.o setcore.o
C_FILES = setcore.c
OBJECTS  = $(foreach obj, $(OBJS),$(obj).cmo)
XOBJECTS = $(foreach obj, $(OBJS),$(obj).cmx)
LIBS = $(NAME).cma $(NAME).cmxa $(NAME).cmxs
OCAMLC   = ocamlfind ocamlc -annot
OCAMLOPT = ocamlfind ocamlopt -annot
OCAMLMKLIB = ocamlmklib
OCAMLDEP = ocamldep

SOURCES = $(foreach obj, $(OBJS),$(obj).ml) $(C_FILES)

TOOLS = Makefile

ARCHIVE  = $(NAME).cma
XARCHIVE = $(NAME).cmxa
SARCHIVE = $(NAME).cmxs

REQUIRES = extlib unix bigarray 

all: $(ARCHIVE) $(XARCHIVE) $(SARCHIVE)

.PHONY: install
install: $(LIBS) META
	ocamlfind remove $(NAME)
	ocamlfind install $(NAME) META $(INTF) $(LIBS) *.a *.mli $(C_OBJS)

.PHONY: uninstall
uninstall:
	ocamlfind remove $(NAME)

$(ARCHIVE): $(INTF) $(OBJECTS) $(C_OBJS) 
	$(OCAMLMKLIB) -o $(NAME) $(OBJECTS) $(C_OBJS)

$(XARCHIVE): $(INTF) $(XOBJECTS) $(C_OBJS)
	$(OCAMLMKLIB) -o $(NAME) $(XOBJECTS) $(C_OBJS)

$(SARCHIVE): $(INTF) $(XOBJECTS) $(C_OBJS)
	$(OCAMLOPT) -shared -o $(SARCHIVE) $(C_OBJS) $(XOBJECTS)

bytearray_stubs.o: bytearray_stubs.c
	ocamlc -c bytearray_stubs.c

setcore.o: setcore.c
	ocamlc -c -cc "gcc -D_GNU_SOURCE -o setcore.o -fPIC" setcore.c

version.ml: $(SOURCES) $(TOOLS)
	echo 'let git_version="'`git rev-parse HEAD`'";;' > version.ml

.SUFFIXES: .cmo .cmi .cmx .ml .mli

.ml.cmo:
	$(OCAMLC) -package "$(REQUIRES)" -c $<
.mli.cmi:
	$(OCAMLC) -package "$(REQUIRES)" -c $<
.ml.cmx:
	$(OCAMLOPT) -package "$(REQUIRES)" -c $<

depend: *.ml *.mli
	$(OCAMLDEP) *.ml *.mli >.depend

include .depend

.PHONY: clean
clean:
	rm -f *.cmi *.cmo *.cmx *.cma *.cmxa *.cmxs *.a *.o *.so *.annot

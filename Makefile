NAME = parmap
OBJS = $(NAME)
INTF = $(foreach obj, $(OBJS),$(obj).cmi)
OBJECTS  = $(foreach obj, $(OBJS),$(obj).cmo)
XOBJECTS = $(foreach obj, $(OBJS),$(obj).cmx)
LIBS = $(NAME).cma $(NAME).cmxa $(NAME).cmxs
OCAMLC   = ocamlfind ocamlc -annot
OCAMLOPT = ocamlfind ocamlopt -annot
OCAMLMKLIB = ocamlmklib
OCAMLDEP = ocamldep

ARCHIVE  = $(NAME).cma
XARCHIVE = $(NAME).cmxa
SARCHIVE = $(NAME).cmxs

REQUIRES = extlib unix bigarray 

all: $(ARCHIVE) $(XARCHIVE) $(SARCHIVE)

.PHONY: install
install: $(LIBS) META
	ocamlfind remove $(NAME)
	ocamlfind install $(NAME) META $(INTF) $(LIBS) *.a *.mli

.PHONY: uninstall
uninstall:
	ocamlfind remove $(NAME)

$(ARCHIVE): $(INTF) $(OBJECTS)
	$(OCAMLMKLIB) -o $(NAME) $(OBJECTS)

$(XARCHIVE): $(INTF) $(XOBJECTS)
	$(OCAMLMKLIB) -o $(NAME) $(XOBJECTS)

$(SARCHIVE): $(INTF) $(XOBJECTS)
	$(OCAMLOPT) -shared -o $(SARCHIVE) $(XOBJECTS)

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

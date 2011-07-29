NAME = parmap
OBJS = $(NAME)
INTF = $(foreach obj, $(OBJS),$(obj).cmi)
C_OBJS = ba_marshal_stubs
OBJECTS  = $(foreach obj, $(OBJS),$(obj).cmo)
XOBJECTS = $(foreach obj, $(OBJS),$(obj).cmx)
LIBS = $(NAME).cma $(NAME).cmxa
OCAMLC   = ocamlfind ocamlc
OCAMLOPT = ocamlfind ocamlopt
OCAMLDEP = ocamldep

ARCHIVE  = $(NAME).cma
XARCHIVE = $(NAME).cmxa

REQUIRES = unix bigarray

all: $(ARCHIVE) $(XARCHIVE) 

.PHONY: install
install: $(LIBS) META
	ocamlfind remove $(NAME)
	ocamlfind install $(NAME) META $(INTF) $(LIBS) *.a

.PHONY: uninstall
uninstall:
	ocamlfind remove $(NAME)

$(ARCHIVE): $(OBJECTS)
	$(OCAMLC) -a -o $(ARCHIVE) -package "$(REQUIRES)" $(OBJECTS)
$(XARCHIVE): $(XOBJECTS)
	$(OCAMLOPT) -a -o $(XARCHIVE) -package "$(REQUIRES)" $(XOBJECTS)

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
	rm -f *.cmi *.cmo *.cmx *.cma *.cmxa *.a *.o

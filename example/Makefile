OCAMLBUILD=ocamlbuild
OBFLAGS := -j 10 -use-ocamlfind
OCAMLBEST=native

TARGETS = \
  mandels.$(OCAMLBEST)

all:
	$(OCAMLBUILD) $(OBFLAGS) $(TARGETS)

.PHONY: clean
clean:
	$(OCAMLBUILD) -clean


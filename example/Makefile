OCAMLBUILD=ocamlbuild
OBFLAGS := -j 10 -use-ocamlfind
OCAMLBEST=native

TARGETS = \
  mandels.$(OCAMLBEST)

all:
	$(OCAMLBUILD) $(OBFLAGS) $(TARGETS)

mandels_sdl.native: mandels_sdl.ml
	ocamlfind ocamlopt -linkpkg -package bigarray -package graphics -package parmap -package unix -package sdl mandels_sdl.ml -o mandels_sdl.native

.PHONY: clean
clean:
	$(OCAMLBUILD) -clean


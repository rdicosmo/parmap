OCAMLBUILD=ocamlbuild
OBFLAGS := -j 10 -use-ocamlfind
OCAMLBEST=native

TARGETS = \
  simplescale.$(OCAMLBEST) \
  floatscale.$(OCAMLBEST) \
  simplescale_array.$(OCAMLBEST) \
  simplescalefold.$(OCAMLBEST) \
  simplescalemapfold.$(OCAMLBEST) \
  mandels.$(OCAMLBEST)

all:
	$(OCAMLBUILD) $(OBFLAGS) $(TARGETS)

 .PHONY: clean
 clean:
	$(OCAMLBUILD) -clean

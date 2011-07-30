all: simplescale.native mandels.native

simplescale.native: simplescale.ml
	ocamlfind ocamlopt -o simplescale.native -package parmap -linkpkg simplescale.ml

mandels.native: mandels.ml
	ocamlfind ocamlopt -o mandels.native -package "graphics parmap" -linkpkg mandels.ml

.PHONY: clean
clean:
	rm -f mandels.native simplescale.native *.cmi *.cmo *.cmx *.cma *.cmxa *.a *.o

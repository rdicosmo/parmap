all: simplescale.native simplescale_array.native simplescalefold.native simplescalemapfold.native mandels.native

simplescale.native: simplescale.ml
	ocamlfind ocamlopt -o simplescale.native -package "extlib parmap" -linkpkg simplescale.ml

simplescale_array.native: simplescale_array.ml
	ocamlfind ocamlopt -o simplescale_array.native -package "extlib parmap" -linkpkg simplescale_array.ml

simplescalefold.native: simplescalefold.ml
	ocamlfind ocamlopt -o simplescalefold.native -package "extlib parmap" -linkpkg simplescalefold.ml

simplescalemapfold.native: simplescalemapfold.ml
	ocamlfind ocamlopt -o simplescalemapfold.native -package "extlib parmap" -linkpkg simplescalemapfold.ml

mandels.native: mandels.ml
	ocamlfind ocamlopt -o mandels.native -package "extlib graphics parmap" -linkpkg mandels.ml

.PHONY: clean
clean:
	rm -f *.native *.cmi *.cmo *.cmx *.cma *.cmxa *.a *.o *~

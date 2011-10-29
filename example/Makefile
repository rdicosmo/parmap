all: simplescale.native simplescale_array.native simplescalefold.native simplescalemapfold.native mandels.native

simplescale.native: utils.ml simplescale.ml
	ocamlfind ocamlopt -o simplescale.native -package "extlib parmap" -linkpkg utils.ml simplescale.ml

simplescale_array.native: utils.ml simplescale_array.ml
	ocamlfind ocamlopt -o simplescale_array.native -package "extlib parmap" -linkpkg utils.ml simplescale_array.ml

simplescalefold.native: utils.ml simplescalefold.ml
	ocamlfind ocamlopt -o simplescalefold.native -package "extlib parmap" -linkpkg utils.ml simplescalefold.ml

simplescalemapfold.native: utils.ml simplescalemapfold.ml
	ocamlfind ocamlopt -o simplescalemapfold.native -package "extlib parmap" -linkpkg utils.ml simplescalemapfold.ml

mandels.native: utils.ml mandels.ml
	ocamlfind ocamlopt -o mandels.native -package "extlib graphics parmap" -linkpkg utils.ml mandels.ml

.PHONY: clean
clean:
	rm -f *.native *.cmi *.cmo *.cmx *.cma *.cmxa *.a *.o *~

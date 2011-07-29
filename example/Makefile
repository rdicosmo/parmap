simplescale.native: simplescale.ml
	ocamlfind ocamlopt -o simplescale.native -package parmap -linkpkg simplescale.ml
.PHONY: clean
clean:
	rm -f simplescale.native *.cmi *.cmo *.cmx *.cma *.cmxa *.a *.o

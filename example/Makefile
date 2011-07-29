simplescale.native: simplescale.ml
	ocamlfind ocamlopt -o simplescale.native -package "unix bigarray mmap parmap" -linkpkg simplescale.ml
.PHONY: clean
clean:
	rm -f simplescale.native *.cmi *.cmo *.cmx *.cma *.cmxa *.a *.o

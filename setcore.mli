(* uses the native affinity interface to 
   declare that the current process should be
   attached to core number n *)

external setcore: int -> unit = "setcore"

(**************************************************************************)
(* ParMap: a simple library to perform Map computations on a multi-core   *)
(*                                                                        *)
(*  Author(s):  Marco Danelutto, Roberto Di Cosmo                         *)
(*                                                                        *)
(*  This library is free software: you can redistribute it and/or modify  *)
(*  it under the terms of the GNU Lesser General Public License as        *)
(*  published by the Free Software Foundation, either version 3 of the    *)
(*  License, or (at your option) any later version.  A special linking    *)
(*  exception to the GNU Lesser General Public License applies to this    *)
(*  library, see the LICENSE file for more information.                   *)
(**************************************************************************)

(** Module [Parmap]: parallel map on multicores. *)

(** {6 Parallel mapfold} *)

val parmapfold : ?ncores:int -> ('a -> 'b) -> 'a list -> ('b-> 'c -> 'c) -> 'c -> ('c->'c->'c) -> 'c

  (** [parmapfold ~ncores:n f l op b concat ] computes [List.fold_right op (List.map f l) b] 
      by forking [n] processes on a multicore machine. 
      You need to provide the extra [concat] operator to combine the partial results of the
      fold computed on each core. If 'b = 'c, then [concat] may be simply [op]. 
      The order of computation in parallel changes w.r.t. sequential execution, so this 
      function is only correct if [op] and [concat] are associative and commutative.
      *)

(** {6 Parallel fold} *)
val parfold: ?ncores:int -> ('a -> 'b -> 'b) -> 'a list -> 'b -> ('b->'b->'b) -> 'b
  (** [parfold ~ncores:n op l b concat] computes [List.fold_right op l b] 
      by forking [n] processes on a multicore machine.
      You need to provide the extra [concat] operator to combine the partial results of the
      fold computed on each core. If 'b = 'c, then [concat] may be simply [op]. 
      The order of computation in parallel changes w.r.t. sequential execution, so this 
      function is only correct if [op] and [concat] are associative and commutative.
      *)

(** {6 Parallel map} *)

val parmap : ?ncores:int -> ('a -> 'b) -> 'a list -> 'b list
  (** [parmap  ~ncores:n f l ] computes [List.map f l] 
      by forking [n] processes on a multicore machine. *)

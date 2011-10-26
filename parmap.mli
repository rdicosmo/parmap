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

(** {6 Sequence type, subsuming lists and arrays} *)

type 'a sequence = L of 'a list | A of 'a array;;


(** {6 Parallel mapfold} *)

val parmapfold : ?ncores:int -> ?chunksize:int -> ('a -> 'b) -> 'a sequence -> ('b-> 'c -> 'c) -> 'c -> ('c->'c->'c) -> 'c

  (** [parmapfold ~ncores:n f (L l) op b concat ] computes [List.fold_right op (List.map f l) b] 
      by forking [n] processes on a multicore machine. 
      You need to provide the extra [concat] operator to combine the partial results of the
      fold computed on each core. If 'b = 'c, then [concat] may be simply [op]. 
      The order of computation in parallel changes w.r.t. sequential execution, so this 
      function is only correct if [op] and [concat] are associative and commutative.
      [parmapfold ~ncores:n f (A a) op b concat ] computes [Array.fold_right op (Array.map f a) b] 
      *)

(** {6 Parallel fold} *)
val parfold: ?ncores:int -> ?chunksize:int -> ('a -> 'b -> 'b) -> 'a sequence -> 'b -> ('b->'b->'b) -> 'b
  (** [parfold ~ncores:n op (L l) b concat] computes [List.fold_right op l b] 
      by forking [n] processes on a multicore machine.
      You need to provide the extra [concat] operator to combine the partial results of the
      fold computed on each core. If 'b = 'c, then [concat] may be simply [op]. 
      The order of computation in parallel changes w.r.t. sequential execution, so this 
      function is only correct if [op] and [concat] are associative and commutative.
      [parfold ~ncores:n op (A a) b concat] similarly computes [Array.fold_right op a b].
      *)

(** {6 Parallel map} *)

val parmap : ?ncores:int -> ?chunksize:int -> ('a -> 'b) -> 'a sequence -> 'b list
  (** [parmap  ~ncores:n f (L l) ] computes [List.map f l] 
      by forking [n] processes on a multicore machine.
      [parmap  ~ncores:n f (A a) ] computes [Array.map f a] 
      by forking [n] processes on a multicore machine. *)

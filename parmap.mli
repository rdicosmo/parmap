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

(** {6 Parallel map} *)

val parmap : ('a -> 'b) -> 'a list -> ?ncores:int -> 'b list
  (** [parmap f l ~ncores:n ] computes [List.map f l] 
      by forking [n] processes on a mulicore machine. *)

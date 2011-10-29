(**************************************************************************)
(* Sample use of Parmap, a simple library to perform Map computations on  *)
(* a multi-core                                                           *)
(*                                                                        *)
(*  Author(s):  Roberto Di Cosmo                                          *)
(*                                                                        *)
(*  This program is free software: you can redistribute it and/or modify  *)
(*  it under the terms of the GNU General Public License as               *)
(*  published by the Free Software Foundation, either version 2 of the    *)
(*  License, or (at your option) any later version.                       *)
(**************************************************************************)

open Parmap
open Utils
let initsegm n = let rec aux acc = function 0 -> acc | n -> aux (n::acc) (n-1) in aux [] n
;;

let compute p = 
  let r=ref 1 in 
  for i = 1 to 80000 do 
    r:= !r+(p*p)-(p*(p-1))
  done;
  !r
;;

scale_test compute (L (initsegm 20000)) 2 1 10;;


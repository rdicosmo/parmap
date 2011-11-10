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

let fcompute p = 
  let r=ref 1. in 
  for i = 1 to 80000 do 
    r:= !r+.(p*.p)-.(p*.(p-.1.))
  done;
  !r
;;

Printf.printf "*** Computations on integer lists\n";

scale_test compute (L (initsegm 20000)) 2 1 10;;

Printf.printf "*** Computations on integer lists (chunksize=100)\n";

scale_test ~chunksize:100 ~inorder:false compute (L (initsegm 20000)) 2 1 10;;

Printf.printf "*** Computations on integer arrays\n";

scale_test compute (A (Array.init 20000 (fun n -> n+1))) 2 1 10;;

Printf.printf "*** Computations on integer arrays (chunksize-100)\n";

scale_test ~chunksize:100 ~inorder:false compute (A (Array.init 20000 (fun n -> n+1))) 2 1 10;;

Printf.printf "*** Computations on lists of floats\n";

scale_test fcompute (L (List.map float_of_int (initsegm 20000))) 2 1 10;;

Printf.printf "*** Computations on lists of floats (chunksize=100)\n";

scale_test  ~chunksize:100 ~inorder:false fcompute (L (List.map float_of_int (initsegm 20000))) 2 1 10;;

Printf.printf "*** Computations on arrays of floats\n";

scale_test fcompute (A (Array.init 20000 (fun n -> float_of_int (n+1)))) 2 1 10;;

Printf.printf "*** Computations on arrays of floats (chunksize=100)\n";

scale_test  ~chunksize:100 ~inorder:false fcompute (A (Array.init 20000 (fun n -> float_of_int (n+1)))) 2 1 10;;


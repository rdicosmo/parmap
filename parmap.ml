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
(*  library, see the COPYING file for more information.                   *)
(**************************************************************************)

open Common
open Util

(* the parallel map function *)

let parmap (f:'a -> 'b) (l:'a list) ?(ncores=1) : 'b list=
  let t = Timer.create "collection" in
  Timer.enable "collection";
  let tc = Timer.create "computation" in
  Timer.enable "computation";
  Timer.start tc;
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = List.length l in
  let chunksize = ln/ncores in
  let pidl = ref [] in
  for i = 0 to ncores-1 do
    match Unix.fork() with
      0 -> 
	begin
          let reschunk=ref [] in
          let limit=if i=ncores-1 then ln-1 else (i+1)*chunksize-1 in
          for j=i*chunksize to limit do
	    try 
              reschunk := (f (List.nth l j))::!reschunk
	    with _ -> (Printf.printf "Error: j=%d\n" j)
          done;
          Printf.eprintf "Process %d done computing\n" (Unix.getpid()); flush stderr;
          let oc = open_out (Printf.sprintf ".res_%d" i) in
	    Marshal.to_channel oc (Unix.getpid(),(List.rev !reschunk)) [Marshal.Closures]; close_out oc;
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> (* collect the worker io data *)
	(pidl:= pid::!pidl) 
  done;
  (* wait for all childrens *)
  List.iter (fun _ -> try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> ()) !pidl;
  (* read in all data *)
  let res = ref [] in
  for i = 0 to ncores-1 do
    let fn = (Printf.sprintf ".res_%d" i) in
    let ic = open_in fn in
      res:= (Marshal.from_channel ic)::!res; close_in ic; (* Unix.unlink fn *)
  done;
  Timer.stop tc ();  Timer.pp_timer Format.std_formatter tc;
  (* is _this_ taking too much time? *)
  Timer.start t;
  let l = 
    List.flatten (List.map (fun pid -> List.assoc pid !res) (List.rev !pidl))
  in 
  Timer.stop t (); Timer.pp_timer Format.std_formatter t;
  l 
;;


(* example:
List.iter (fun n -> Printf.printf "%d\n" n) (parmap (fun x -> x+1) [1;2;3;4;5;6;7;8;9;10;11;12;13] ~ncores:4);;
 *)

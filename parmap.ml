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



(* create a shadow file descriptor *)

let tempfd () =
  let name = Filename.temp_file "data" "TMP" in
  try
    let fd = Unix.openfile name [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
    Unix.unlink name;
    fd
  with e -> Unix.unlink name; raise e

(* unmarshal from a mmap seen as a bigarray *)
let unmarshal_from_mmap fd =
 let read_mmap ofs len = 
   let s = String.make len ' ' in
   for k = 0 to len-1 do s.[k]<-fd.{ofs+k} done;
   s
 in
 (* read the header *)
 let s = read_mmap 0 Marshal.header_size in
 let size=Marshal.total_size s 0 in
 let s' = read_mmap 0 size in
 Marshal.from_string s' 0
;;


(* the parallel map function *)

let parmap (f:'a -> 'b) (l:'a list) ?(ncores=1) : 'b list=
  let t = Timer.create "collection" in
  Timer.enable "collection";
  let tc = Timer.create "computation" in
  Timer.enable "computation";
  let tm = Timer.create "marshalling" in
  Timer.enable "marshalling";
  Timer.start tc;
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = List.length l in
  let chunksize = ln/ncores in
  let maxsize = 15000000 in
  let fdarr=Array.init ncores (fun _ -> Bigarray.Array1.map_file (tempfd()) Bigarray.char Bigarray.c_layout true maxsize) in
  for i = 0 to ncores-1 do
       match Unix.fork() with
      0 -> 
	begin
          let pid = Unix.getpid() in
          let reschunk=ref [] in
          let limit=if i=ncores-1 then ln-1 else (i+1)*chunksize-1 in
          for j=i*chunksize to limit do
	    try 
              reschunk := (f (List.nth l j))::!reschunk
	    with _ -> (Printf.printf "Error: j=%d\n" j)
          done;
          Printf.eprintf "Process %d done computing\n" pid; flush stderr;
          Timer.start tm;
          let s = Marshal.to_string (List.rev !reschunk) [Marshal.Closures] in
          let sl = (String.length s) in
          Timer.stop tm (); Timer.pp_timer Format.std_formatter tm;
          Printf.eprintf "Process %d has marshaled result of size %d\n" pid sl;
	  for k = 0 to sl-1 do fdarr.(i).{k} <-s.[k] done;
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> ()
  done;
  (* wait for all childrens *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;
  (* read in all data *)
  let res = ref [] in
  for i = 0 to ncores-1 do
      res:= (unmarshal_from_mmap fdarr.(i)) ::!res;
  done;
  Timer.stop tc ();  Timer.pp_timer Format.std_formatter tc;
  (* is _this_ taking too much time? *)
  Timer.start t;
  let l = 
    List.flatten (List.rev !res)
  in 
  Timer.stop t (); Timer.pp_timer Format.std_formatter t;
  l 
;;


(* example:
List.iter (fun n -> Printf.printf "%d\n" n) (parmap (fun x -> x+1) [1;2;3;4;5;6;7;8;9;10;11;12;13] ~ncores:4);;
 *)

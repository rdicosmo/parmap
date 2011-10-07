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

open ExtLib

(* unmarshal from a mmap seen as a bigarray *)
let unmarshal fd =
 let a=Bigarray.Array1.map_file fd Bigarray.char Bigarray.c_layout true (-1) in
 let read_mmap ofs len = 
   let s = String.make len ' ' in
   for k = 0 to len-1 do s.[k]<-a.{ofs+k} done;
   s
 in
 (* read the header *)
 let s = read_mmap 0 Marshal.header_size in
 let size=Marshal.total_size s 0 in
 let s' = read_mmap 0 size in
 Unix.close fd;
 Marshal.from_string s' 0


(* marshal to a mmap seen as a bigarray *)

let marshal pid fd v = 
  let s = Marshal.to_string v [Marshal.Closures] in
  let sl = (String.length s) in
  let ba = Bigarray.Array1.map_file fd Bigarray.char Bigarray.c_layout true sl in
  for k = 0 to sl-1 do ba.{k} <-s.[k] done;
  Unix.close fd

(* create a shadow file descriptor *)

let tempfd () =
  let name = Filename.temp_file "mmap" "TMP" in
  try
    let fd = Unix.openfile name [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
    Unix.unlink name;
    fd
  with e -> Unix.unlink name; raise e

type 'a sequence = L of 'a list | A of 'a array;;

(* the core parallel mapfold function *)

let parmapfold ?(ncores=1) (f:'a -> 'b) (s:'a sequence) (op:'b->'c->'c) (opid:'c) (concat:'c->'c->'c) : 'c=
  (* flush everything *)
  flush stdout; flush stderr;
  (* enforce array to speed up access to the list elements *)
  let al = 
    match s with 
      A al -> al
    | L l  -> Array.of_list l 
  in
  (* init task parameters *)
  let ln = Array.length al in
  let chunksize = ln/ncores in
  let fdarr=Array.init ncores (fun _ -> tempfd()) in
  for i = 0 to ncores-1 do
       match Unix.fork() with
      0 -> 
	begin
          let pid = Unix.getpid() in
          let reschunk=ref opid in
          let lo=i*chunksize in
          let hi=if i=ncores-1 then ln-1 else (i+1)*chunksize-1 in
          (* iterate in reverse order, to accumulate in the right order *)
          for j=0 to (hi-lo) do
	    try 
              reschunk := op (f (al.(hi-j))) !reschunk
	    with e -> (Printf.printf "Error: at index j=%d got exception %s\n" j (Printexc.to_string e))
          done;
	  marshal pid fdarr.(i) (!reschunk:'d);
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> ()
  done;
  (* wait for all childrens *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;
  (* read in all data *)
  let res = ref [] in
  (* iterate in reverse order, to accumulate in the right order *)
  for i = 0 to ncores-1 do
      res:= ((unmarshal fdarr.((ncores-1)-i)):'d)::!res;
  done;
  (* use extLib's tail recursive one *)
  List.fold_right concat !res opid
;;

(* the parallel map function *)

let parmap ?(ncores=1) (f:'a -> 'b) (s:'a sequence) : 'b list=
    parmapfold f s (fun v acc -> v::acc) [] ~ncores (@) 
;;

(* the parallel fold function *)

let parfold ?(ncores=1) (op:'a -> 'b -> 'b) (s:'a sequence) (opid:'b) (concat:'b->'b->'b) : 'b=
    parmapfold ~ncores (fun x -> x) s op opid concat
;;

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

open Util

module type MemoryMarshal =
    sig
      val unmarshal : Unix.file_descr -> 'a
      val marshal : int -> Unix.file_descr -> 'a -> unit
    end

module MmapBigArray : MemoryMarshal =
  struct
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
  Printf.eprintf "Process %d has marshaled result of size %d\n" pid sl;
  let ba = Bigarray.Array1.map_file fd Bigarray.char Bigarray.c_layout true sl in
  for k = 0 to sl-1 do ba.{k} <-s.[k] done;
  Unix.close fd
end

module MmapXenTools : MemoryMarshal =
  struct
(* unmarshal from a mmap as given by Xen tools *)
let unmarshal fd =
  (* read the header, to find out the size of the rest of the data *)
   let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED Marshal.header_size 0 in 
   let header = Mmap.read mm 0 Marshal.header_size in
   Mmap.unmap mm;
   let size = Marshal.total_size header 0 in
  (* now map the full length of the data, and do the unmarshaling *)
   let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED size 0 in 
   let s = Mmap.read mm 0 size in
   Mmap.unmap mm;
   Unix.close fd;
   Marshal.from_string s 0
 
(* marshaling to a fd using a mmap as given by Xen tools *)

let marshal pid fd v = 
  let s = Marshal.to_string v [Marshal.Closures] in
  let sl = (String.length s) in
  (* stretch the file to the required size *)
  ignore(Unix.lseek fd sl Unix.SEEK_SET); 
  ignore(Unix.write fd "0" 0 1);
  (* rewind the file, probably not needed *)
  ignore(Unix.lseek fd 0 Unix.SEEK_SET); 
  (* create the mmap *)
  let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED sl 0 in
  (* write marshal output on the mmap *)
  Mmap.write mm s 0 sl;
  Unix.close fd
end

module Parmap = 
  functor (M: MemoryMarshal) ->
  struct

(* timers *)

let t = Timer.create "collection";;
Timer.enable "collection";;
let tc = Timer.create "computation";;
Timer.enable "computation";;
let tm = Timer.create "marshalling";;
Timer.enable "marshalling";;
let tu = Timer.create "unmarshalling";;
Timer.enable "unmarshalling";;

(* create a shadow file descriptor *)

let tempfd () =
  let name = Filename.temp_file "data" "TMP" in
  try
    let fd = Unix.openfile name [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
    Unix.unlink name;
    fd
  with e -> Unix.unlink name; raise e

(* the parallel map function *)

let parmap (f:'a -> 'b) (l:'a list) ?(ncores=1) : 'b list=
  Timer.start tc;
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = List.length l in
  let chunksize = ln/ncores in
  let fdarr=Array.init ncores (fun _ -> tempfd()) in
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
	  M.marshal pid fdarr.(i) (List.rev !reschunk);
          Timer.stop tm (); Timer.pp_timer Format.std_formatter tm;
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
      res:= (M.unmarshal fdarr.(i)) ::!res;
  done;
  Timer.stop tc ();  Timer.pp_timer Format.std_formatter tc;
  List.flatten (List.rev !res)

end

module PmapBA = Parmap(MmapBigArray);;
module PmapXT = Parmap(MmapXenTools);;

let parmap = PmapBA.parmap;;

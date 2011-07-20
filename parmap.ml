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

(* timers *)

let t = Timer.create "collection";;
Timer.enable "collection";;
let tc = Timer.create "computation";;
Timer.enable "computation";;
let tm = Timer.create "marshalling";;
Timer.enable "marshalling";;

(* create a shadow file descriptor *)

let tempfd () =
  let name = Filename.temp_file "data" "TMP" in
  try
    let fd = Unix.openfile name [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
    Unix.unlink name;
    fd
  with e -> Unix.unlink name; raise e

(* unmarshaling from a fd using a mmap *)

let unmarshal_using_mmap fd =
  (* read the header, to find out the size of the rest of the data *)
  let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED Marshal.header_size 0 in 
  let header = Mmap.read mm 0 Marshal.header_size in
  Mmap.unmap mm;
  let size = Marshal.total_size header 0 in
  (* now map the full length of the data, and do the unmarshaling *)
  Printf.eprintf "Reading %d bytes from mmap\n" size;
  let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED size 0 in 
  let s = Mmap.read mm 0 size in
  Printf.eprintf "Read %d bytes from mmap\n" size;
  Mmap.unmap mm;
  Marshal.from_string s 0
;;

(* marshaling to a fd using a mmap *)

let marshal_using_mmap pid fd v = 
  Timer.start tm;
  let s = Marshal.to_string v [Marshal.Closures] in
  let sl = (String.length s) in
  Timer.stop tm (); Timer.pp_timer Format.std_formatter tm;
  Printf.eprintf "Process %d has marshaled result of size %d\n" pid sl;
  (* stretch the file to the required size *)
  ignore(Unix.lseek fd sl Unix.SEEK_SET); 
  ignore(Unix.write fd "0" 0 1);
  (* rewind the file, probably not needed *)
  ignore(Unix.lseek fd 0 Unix.SEEK_SET); 
  (* create the mmap *)
  let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED sl 0 in
  (* write marshal output on the mmap *)
  Mmap.write mm s 0 sl;
  Printf.eprintf "Process %d has successfully written result of size %d\n" pid sl;;

(* the parallel map function *)

let parmap (f:'a -> 'b) (l:'a list) ?(ncores=1) : 'b list=
  Timer.start tc;
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = List.length l in
  let chunksize = ln/ncores in
  (* create the file descriptors used for mmapping *)
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
          marshal_using_mmap pid fdarr.(i) (List.rev !reschunk);
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> ()

  done;
  (* wait for all childrens *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;
  Timer.stop tc (); Timer.pp_timer Format.std_formatter tc;
  (* read in all data *)
  Timer.start t;
  let res = ref [] in
  for i = 0 to ncores-1 do
      let v = unmarshal_using_mmap fdarr.(i) in 
      res:= v::!res;
  done;
  let l = List.flatten (List.rev !res) in
  Timer.stop t (); Timer.pp_timer Format.std_formatter tc;
  l
;;


(* example:
List.iter (fun n -> Printf.printf "%d\n" n) (parmap (fun x -> x+1) [1;2;3;4;5;6;7;8;9;10;11;12;13] ~ncores:4);;
 *)

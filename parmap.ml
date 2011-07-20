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

(* informations from child to parents *)

type outcome = Res of payload | Fail of exn
and payload = int * int (* pid + size of result *)

(* create a pipe, and return the access functions, as well as the underlying file descriptor *)

let mkpipe () =
    let (ifd,ofd) = Unix.pipe () in
    let (ic, oc) = (Unix.in_channel_of_descr ifd, Unix.out_channel_of_descr ofd) in
    ((fun x ->  Marshal.to_channel oc x [Marshal.Closures]; flush oc),
     (fun () -> Marshal.from_channel ic),
     (fun () -> close_out oc),
     (fun () -> close_in ic),
     ifd
    )
;;  

(* create a shadow file descriptor *)

let tempfd () =
  let name = Filename.temp_file "data" "TMP" in
  try
    let fd = Unix.openfile name [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
    Unix.unlink name;
    fd
  with e -> Unix.unlink name; raise e

(* readers *)

type 'a reader = (unit -> 'a) option

(* unmarshaling from a mmap *)

let unmarshal_using_mmap fd =
  let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED Marshal.header_size 0 in 
  let header = Mmap.read mm 0 Marshal.header_size in
  Mmap.unmap mm;
  let size = Marshal.total_size header 0 in
  Printf.eprintf "Reading %d bytes from mmap\n" size;
  let mm = Mmap.mmap fd Mmap.RDWR Mmap.SHARED size 0 in 
  let s = Mmap.read mm 0 size in
  Printf.eprintf "Read %d bytes from mmap\n" size;  
  Mmap.unmap mm;
  Marshal.from_string s 0
;;

let map_size = 60000000;;

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
  let readers = Array.init ncores (fun _ -> None) in
  (* create the file descriptors used for mmapping *)
  let fdarr=Array.init ncores (fun _ -> tempfd()) in
  for i = 0 to ncores-1 do
    (* create a pipe for writing back the results *)
    let (writep,readp,closew,closer,ifd) = mkpipe() in
       match Unix.fork() with
      0 -> 
	begin
          closer();
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
          (* stretch the file to the required size *)
          ignore(Unix.lseek fdarr.(i) sl Unix.SEEK_SET); 
          ignore(Unix.write fdarr.(i) "0" 0 1);
          (* rewind the file *)
          ignore(Unix.lseek fdarr.(i) 0 Unix.SEEK_SET); 
          (* create the mmap *)
          let mm = Mmap.mmap fdarr.(i) Mmap.RDWR Mmap.SHARED sl 0 in
          (* write marshal output on the mmap *)
	  Mmap.write mm s 0 sl;
          Printf.eprintf "Process %d has successfully written result of size %d\n" pid sl;
          let s' = Mmap.read mm 0 sl in
          if s=s' then
            Printf.eprintf "Process %d has successfully re-read and verified result of size %d\n" pid sl
          else 
            Printf.eprintf "Process %d has successfully re-read result of size %d\n" pid sl;
          writep sl; 
          closew();
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> (* collect the worker io data *)
	(closew();(readers.(i)<- Some readp))
  done;
  (* wait for all childrens *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;
  (* read in all data *)
  let sizes = Array.init ncores (fun _ -> 0) in
  let maxs = ref 0 in
  for i=0 to ncores-1 do
    match readers.(i) 
    with 
      Some reader -> sizes.(i)<-reader(); maxs := max sizes.(i) !maxs
    | _ -> failwith (Printf.sprintf "No registered reader at index %d." i)
  done;
  (* copy-buffer option *)
  let res = ref [] in
  for i = 0 to ncores-1 do
      let v = unmarshal_using_mmap fdarr.(i) in 
      res:= v::!res;
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

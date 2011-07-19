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

(* the worker io data structure *)

type 'a wio = {pid:int; ifd:Unix.file_descr; reader: unit -> 'a}

(* select ready wios *)

let select_wios wiol =
  let inset = List.map (fun wio -> wio.ifd) wiol in
  let ready, _, _ = Unix.select inset [] [] (-1.) in
  List.partition (fun wio -> List.memq wio.ifd ready) wiol

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
  let wiol = ref [] in
  for i = 0 to ncores-1 do
    (* create a pipe for writing back the results *)
    let (writep,readp,closew,closer,ifd) = mkpipe() in
    match Unix.fork() with
      0 -> 
	begin
	  closer();
          let reschunk=ref [] in
          let limit=if i=ncores-1 then ln-1 else (i+1)*chunksize-1 in
          for j=i*chunksize to limit do
	    try 
              reschunk := (f (List.nth l j))::!reschunk
	    with _ -> (Printf.printf "Error: j=%d\n" j)
          done;
          Printf.eprintf "Process %d done computing\n" (Unix.getpid()); flush stderr;
          writep (List.rev !reschunk); closew();
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> (* collect the worker io data *)
	(closew(); wiol:= {pid=pid;reader=readp;ifd=ifd}::!wiol) 
  done;
  (* now do read all the results using a select *)
  let res = ref [] in
  let waiting = ref !wiol in
  while !waiting != [] do
    let idle,busy=select_wios !waiting in
    List.iter 
      (fun wio -> 
	try 
	  res:= (wio.pid,wio.reader())::!res
	with e -> (Printf.printf "Read error on pid %d\n" wio.pid; raise e)
      ) idle;
    waiting:=busy
  done;
  (* wait for all childrens *)
  List.iter (fun _ -> try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> ()) !wiol;
  Timer.stop tc ();  Timer.pp_timer Format.std_formatter tc;
  (* is _this_ taking too much time? *)
  Timer.start t;
  let l = 
    List.flatten (List.map (fun pid -> List.assoc pid !res) (List.rev (List.map (fun x -> x.pid) !wiol)))
  in 
  Timer.stop t (); Timer.pp_timer Format.std_formatter t;
  l 
;;


(* example:
List.iter (fun n -> Printf.printf "%d\n" n) (parmap (fun x -> x+1) [1;2;3;4;5;6;7;8;9;10;11;12;13] ~ncores:4);;
 *)

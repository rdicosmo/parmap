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
  let tm = Timer.create "marshalling" in
  Timer.enable "marshalling";
  Timer.start tc;
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = List.length l in
  let chunksize = ln/ncores in
  let port=4000 in
  let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let sockaddr = Unix.ADDR_INET (Unix.inet_addr_any, port) in
  Unix.setsockopt sock Unix.SO_REUSEADDR true;
  Unix.setsockopt sock Unix.SO_KEEPALIVE true;
  Unix.setsockopt_int sock Unix.SO_SNDBUF 1000000;
  Unix.setsockopt_int sock Unix.SO_RCVBUF 1000000;
  Unix.bind sock sockaddr;
  Unix.listen sock ncores;
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
          (* now connect back to the parent, and send the results *) 
          let (ic,oc)=Unix.open_connection sockaddr in
          Marshal.to_channel oc (i,List.rev !reschunk) [Marshal.Closures];
          Printf.eprintf "Process %d has marshaled result\n" pid;
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> ()
  done;


  let res = Array.init ncores (fun _ -> []) in

  for i = 0 to ncores-1 do
    let (fd,saddr) = try Unix.accept sock with e -> (Printf.eprintf "Error in accept"; raise e) in (* accepting the connection *)
    let ic=Unix.in_channel_of_descr fd in 
    let (n,v) = Marshal.from_channel ic in
    res.(n)<-v
  done;

  (* wait for all childrens *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;
  Timer.stop tc ();  Timer.pp_timer Format.std_formatter tc;
  Unix.shutdown sock Unix.SHUTDOWN_ALL;
  Unix.close sock;
  List.flatten (Array.to_list res) 
;;


(* example:
List.iter (fun n -> Printf.printf "%d\n" n) (parmap (fun x -> x+1) [1;2;3;4;5;6;7;8;9;10;11;12;13] ~ncores:4);;
 *)

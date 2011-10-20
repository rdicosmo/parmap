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

let debug=false;;

(* utils; would disappear if we use list comprehension from Batteries *)

let ext_intv startv endv =
  let s,e = (min startv endv),(max startv endv) in
  let rec aux acc = function n -> if n=s then n::acc else aux (n::acc) (n-1)
  in aux [] e
;;

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

(* setup a service on a free port *)

let setup_server quesize = 
  let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt sock Unix.SO_REUSEADDR true;
  Unix.setsockopt sock Unix.SO_KEEPALIVE true;
  (* no need yet to perform tuning at this level
    Unix.setsockopt_int sock Unix.SO_SNDBUF 1000;
    Unix.setsockopt_int sock Unix.SO_RCVBUF 1000;
   *)
  let sockaddr = Unix.ADDR_INET (Unix.inet_addr_any, 0) in (* using zero here forces the system to provide a free port *)
  Unix.bind sock sockaddr;
  Unix.listen sock quesize;
  let sockaddr = Unix.getsockname sock in (* get back the address with the chosen port *)
  let port = match sockaddr with
  | Unix.ADDR_INET(_,x) -> x | _ -> assert false
  in (sock, sockaddr, port)
;;


type 'a sequence = L of 'a list | A of 'a array;;

(* the type of messages exchanged between master and workers *)

type msg = Ready | Finished | Task of int;;

(* the core parallel mapfold function *)

let parmapfold ?(ncores=1) ?(chunksize) (f:'a -> 'b) (s:'a sequence) (op:'b->'c->'c) (opid:'c) (concat:'c->'c->'c) : 'c=
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
  let chunksize, ntasks = 
    match chunksize with
      None -> (ln/ncores, ncores)
    | Some v -> (v,ln/v)
  in
  let fdarr=Array.init ncores (fun _ -> tempfd()) in
  (* setup communication channel with the workers *)
  let sock, sockaddr, port = setup_server ncores in
  for i = 0 to ncores-1 do
       match Unix.fork() with
      0 -> 
	begin    
          let d=Unix.gettimeofday() and pid = Unix.getpid() in
          let reschunk=ref opid in
          let (ic,oc)=Unix.open_connection sockaddr in
	  while true do
            (* ask for work until we are finished *)
	    if debug then Printf.eprintf "Sending Ready token (pid=%d)\n%!" pid;
            Marshal.to_channel oc Ready [Marshal.Closures]; flush oc;
            let token = (Marshal.from_channel ic) in
	    if debug then Printf.eprintf "Received token from master (pid=%d)\n%!" pid;
            match token with
	    | Finished -> 
		(marshal pid fdarr.(i) (!reschunk:'d);
		 try Unix.shutdown_connection ic with _ -> (); (* will fail if channel already closed *)
		 exit 0)
	    | Task i -> 
		let lo=i*chunksize in
		let hi=if i=ntasks-1 then ln-1 else (i+1)*chunksize-1 in
		(* iterate in reverse order, to accumulate in the right order *)
		for j=0 to (hi-lo) do
		  try 
		    reschunk := op (f (al.(hi-j))) !reschunk;
		  with e -> 
		    (Printf.eprintf "[Parmap] Error at index j=%d in (%d,%d), chunksize=%d of a total of %d got exception %s on core %d \n"
		       j hi lo chunksize (hi-lo+1) (Printexc.to_string e) i );
		    Printf.eprintf "[Parmap] Worker on core %d, segment (%d,%d) of data of length %d, chunksize=%d finished in %f seconds\n"
		      i hi lo ln chunksize (Unix.gettimeofday() -. d)
		done;
	  done;
	end
    | -1 ->  Printf.eprintf "[Parmap] Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> ()
  done;
  (* get all connections from the workers *)
  let wfdl = 
    List.map 
      (fun i -> 
	let (fd,saddr) = 
	  try Unix.accept sock 
	  with e -> (Printf.eprintf "[Parmap] error accepting connection from worker %i\n%!" i; 
		     raise e) 
	in (if debug then Printf.eprintf "[Parmap] accepted connection from worker %d\n%!" i; fd))
      (ext_intv 0 (ncores-1))
  in
  (* feed workers until all tasks are finished *)
  for i=0 to ntasks-1 do
    if debug then Printf.eprintf "Select for task %d\n%!" i;
    let (wfd::_),_,_ = Unix.select wfdl [] [] (-1.)
    in match Marshal.from_channel (Unix.in_channel_of_descr wfd) with
      Ready -> 
	(if debug then Printf.eprintf "Sending task %d\n%!" i;
         let oc = (Unix.out_channel_of_descr wfd) in
	 (Marshal.to_channel oc (Task i) []); flush oc)
    | _ -> assert false
    (* will need to add some code to properly close down all the channels incrementally *)
  done;
  
  List.iter (fun wfd -> let oc = (Unix.out_channel_of_descr wfd) in (Marshal.to_channel oc Finished []; flush oc; Unix.close wfd)) wfdl;
  
  (* wait for all childrens to terminate *)
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

let parmap ?(ncores=1) ?chunksize (f:'a -> 'b) (s:'a sequence) : 'b list=
    parmapfold ~ncores ?chunksize f s (fun v acc -> v::acc) [] (@) 
;;

(* the parallel fold function *)

let parfold ?(ncores=1) ?chunksize (op:'a -> 'b -> 'b) (s:'a sequence) (opid:'b) (concat:'b->'b->'b) : 'b=
    parmapfold ~ncores ?chunksize (fun x -> x) s op opid concat
;;

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


(* sequence type, subsuming lists and arrays *)

type 'a sequence = L of 'a list | A of 'a array;;

let debug=false;;

(* utils *)

(* would be [? a | a <- startv--endv] using list comprehension from Batteries *)

let ext_intv startv endv =
  let s,e = (min startv endv),(max startv endv) in
  let rec aux acc = function n -> if n=s then n::acc else aux (n::acc) (n-1)
  in aux [] e
;;

(* find index of the first occurrence of an element in a list *)

let index_of e l =
  let rec aux = function
      ([],_) -> raise Not_found
    | (a::r,n) -> if a=e then n else aux (r,n+1)
  in aux (l,0)
;;

(* freopen emulation, from Xavier's suggestion on OCaml mailing list *)

let reopen_out outchan filename =
  flush outchan;
  let fd1 = Unix.descr_of_out_channel outchan in
  let fd2 =
    Unix.openfile filename [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o666 in
  Unix.dup2 fd2 fd1;
  Unix.close fd2

(* unmarshal from a mmap seen as a bigarray *)
let unmarshal fd =
 let a=Bigarray.Array1.map_file fd Bigarray.char Bigarray.c_layout true (-1) in
 let res=Bytearray.unmarshal a 0 in
 Unix.close fd; 
 res


(* marshal to a mmap seen as a bigarray *)

let marshal fd v = 
  let huge_size = 1 lsl 32 in
  let ba = Bigarray.Array1.map_file fd Bigarray.char Bigarray.c_layout true huge_size in
  ignore(Bytearray.marshal_to_buffer ba 0 v [Marshal.Closures]);
  Unix.close fd

(* create a shadow file descriptor *)

let tempfd () =
  let name = Filename.temp_file "mmap" "TMP" in
  try
    let fd = Unix.openfile name [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
    Unix.unlink name;
    fd
  with e -> Unix.unlink name; raise e

(* the type of messages exchanged between master and workers *)

type msg_up = Ready of int | Error of int * string;;
type msg_down = Finished | Task of int;;

(* the core parallel mapfold function *)

let setup_children_chans pipeup pipedown fdarr i = 
  Setcore.setcore i;
  (* send stdout and stderr to a file to avoid mixing output from different cores *)
  reopen_out stdout (Printf.sprintf "stdout.%d" i);
  reopen_out stderr (Printf.sprintf "stderr.%d" i);
  (* close the other ends of the pipe and convert my ends to ic/oc *)
  Unix.close (snd pipedown.(i));Unix.close (fst pipeup.(i));
  let pid = Unix.getpid() in
  let ic = Unix.in_channel_of_descr (fst pipedown.(i))
  and oc = Unix.out_channel_of_descr (snd pipeup.(i)) in
  let receive () = Marshal.from_channel ic in
  let signal v = Marshal.to_channel oc v []; flush oc in
  let return v = 
    let d = Unix.gettimeofday() in 
    let _ = marshal fdarr.(i) v in
    if debug then Printf.eprintf "[Parmap]: worker elapsed %f in marshalling\n%!" (Unix.gettimeofday() -. d) in
  let finish () =
    (if debug then Printf.eprintf "Shutting down (pid=%d)\n%!" pid;
     try close_in ic; close_out oc with _ -> ()
    ); exit 0 in 
  receive, signal, return, finish, pid
;;

(* parametric mapper primitive that captures the parallel structure *)

let mapper ncores ~chunksize compute opid al collect =
  let tstart=Unix.gettimeofday()  in
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = Array.length al in
  let chunksize, ntasks = 
    match chunksize with
      None -> (ln/ncores, ncores)
    | Some v -> (v,ln/v)
  in
  let fdarr=Array.init ncores (fun _ -> tempfd()) in
  (* setup communication channel with the workers *)
  let pipedown=Array.init ncores (fun _ -> Unix.pipe ()) in
  let pipeup=Array.init ncores (fun _ -> Unix.pipe ()) in
  for i = 0 to ncores-1 do
       match Unix.fork() with
      0 -> 
	begin    
          let d=Unix.gettimeofday()  in
          (* primitives for communication *)
          let receive,signal,return,finish,pid = setup_children_chans pipeup pipedown fdarr i in
          let reschunk=ref opid in
          let compute n = 
	    let lo=n*chunksize in
	    let hi=if n=ntasks-1 then ln-1 else (n+1)*chunksize-1 in
            let exc_handler e j = (* handle an exception at index j *)
	      begin
		let errmsg = Printexc.to_string e
		in Printf.eprintf "[Parmap] Error at index j=%d in (%d,%d), chunksize=%d of a total of %d got exception %s on core %d \n%!"
		  j lo hi chunksize (hi-lo+1) errmsg i;
		signal (Error (i,errmsg)); finish()
	      end
            in		    
            reschunk:= compute al lo hi !reschunk exc_handler;
	    Printf.eprintf "[Parmap] Worker on core %d (pid=%d), segment (%d,%d) of data of length %d, chunksize=%d finished in %f seconds\n%!"
	      i pid lo hi ln chunksize (Unix.gettimeofday() -. d)
	  in
	  if ncores=ntasks then (* each worker handles a single chunk *)
	    begin
	      signal (Ready i); 
	      (match receive() with Task n -> compute n | _ -> failwith "Wrong control message in mapper");
	      (match receive() with Finished -> return (!reschunk:'d); finish () | _ -> failwith "Wrong control message in mapper") 
	    end
	  else (* each worker performs a loop handling one chunk after the other *)
	  while true do
            (* ask for work until we are finished *)
            signal (Ready i);
            match receive() with
	    | Finished -> return (!reschunk:'d); finish ()
	    | Task n -> compute n
	  done;
	end
    | -1 ->  Printf.eprintf "[Parmap] Fork error: pid %d; i=%d.\n%!" (Unix.getpid()) i; 
    | pid -> ()
  done;
  (* close unused ends of the pipes *)
  Array.iter (fun (rfd,_) -> Unix.close rfd) pipedown;
  Array.iter (fun (_,wfd) -> Unix.close wfd) pipeup;
  (* get ic/oc/wfdl *)
  let wfdl = List.map fst (Array.to_list pipeup) in
  let ocs=Array.init ncores (fun n -> Unix.out_channel_of_descr (snd pipedown.(n))) in
  let ics=Array.init ncores (fun n -> Unix.in_channel_of_descr (fst pipeup.(n))) in

  (* feed workers until all tasks are finished *)

  (* in case ntasks=ncores, preserve the ordering *)
  let tasksel = if ntasks=ncores then fst else snd in

  if debug then Printf.eprintf "[Parmap]: %f elapsed in forking and setup up to starting the feeder\n%!" (Unix.gettimeofday() -. tstart);
  let tfeedstart=Unix.gettimeofday() in

  for i=0 to ntasks-1 do
    if debug then Printf.eprintf "Select for task %d (ncores=%d, ntasks=%d)\n%!" i ncores ntasks;
    let readyl,_,_ = Unix.select wfdl [] [] (-1.) in
    let wfd=List.hd readyl in (* List.hd never fails here *)
    let w=index_of wfd wfdl
    in match Marshal.from_channel ics.(w) with
      Ready w -> 
	(if debug then Printf.eprintf "Sending task %d to worker %d\n%!" (tasksel (w,i)) w;
         let oc = ocs.(w) in
	 (Marshal.to_channel oc (Task (tasksel(w,i))) []); flush oc)
    | Error (core,msg) -> (Printf.eprintf "[Parmap]: aborting due to exception on core %d: %s\n%!" core msg; exit 1)
  done;
  
  (* send termination token to all children *)
  Array.iter (fun oc -> Marshal.to_channel oc Finished []; flush oc; close_out oc) ocs;

  if debug then Printf.eprintf "[Parmap]: %f elapsed feeding and sending termination tokens\n%!" (Unix.gettimeofday() -. tfeedstart);
  let twait = Unix.gettimeofday() in  

  (* wait for all children to terminate *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;

  if debug then Printf.eprintf "[Parmap]: %f elapsed waiting for children to terminate\n%!" (Unix.gettimeofday() -. twait);
  let tcollect = Unix.gettimeofday() in  

  (* read in all data *)
  let res = ref [] in
  (* iterate in reverse order, to accumulate in the right order *)
  for i = 0 to ncores-1 do
      res:= ((unmarshal fdarr.((ncores-1)-i)):'d)::!res;
  done;
  (* collect all results *)
  let r = collect !res in
  (if debug then Printf.eprintf "[Parmap]: %f elapsed unmarhsalling and collecting\n%!" (Unix.gettimeofday() -. tcollect));  
  r
;;


(* the parallel mapfold function *)

let parmapfold ?(ncores=1) ?(chunksize) (f:'a -> 'b) (s:'a sequence) (op:'b->'c->'c) (opid:'c) (concat:'c->'c->'c) : 'c=
  (* enforce array to speed up access to the list elements *)
  let al = match s with A al -> al | L l  -> Array.of_list l in
  let compute al lo hi previous exc_handler =
    (* iterate in reverse order, to accumulate in the right order *)
    let r = ref previous in
    for j=0 to (hi-lo) do
      try 
	r := op (f (Array.unsafe_get al (hi-j))) !r;
      with e -> exc_handler e j
    done; !r
  in
  mapper ncores ~chunksize compute opid al  (fun r -> List.fold_right concat r opid)
;;

(* the parallel map function *)

let parmap ?(ncores=1) ?chunksize (f:'a -> 'b) (s:'a sequence) : 'b list=
  (* enforce array to speed up access to the list elements *)
  let al = match s with A al -> al | L l  -> Array.of_list l in
  let compute al lo hi previous exc_handler =
    (* iterate in reverse order, to accumulate in the right order, and add to acc *)
    let f' j = try f (Array.unsafe_get al (lo+j)) with e -> exc_handler e j in
    let rec aux acc = 
      function
	  0 ->  (f' 0)::acc
	| n ->  aux ((f' n)::acc) (n-1)
    in aux previous (hi-lo)
  in
  mapper ncores ~chunksize compute [] al  (fun r -> ExtLib.List.concat r)
;;

(* the parallel fold function *)

let parfold ?(ncores=1) ?chunksize (op:'a -> 'b -> 'b) (s:'a sequence) (opid:'b) (concat:'b->'b->'b) : 'b=
    parmapfold ~ncores ?chunksize (fun x -> x) s op opid concat
;;


(* the parallel map function, on arrays *)

let map_intv lo hi f a =
  let l = hi-lo in
  if l < 0 then [||] else begin
    let r = Array.create (l+1) (f(Array.unsafe_get a lo)) in
    for i = 1 to l do
      Array.unsafe_set r i (f(Array.unsafe_get a (lo+i)))
    done;
    r
  end

let array_parmap ?(ncores=1) ?chunksize (f:'a -> 'b) (al:'a array) : 'b array=
  let compute a lo hi previous exc_handler =
    try 
      Array.concat [(map_intv lo hi f a);previous]
    with e -> exc_handler e lo
  in
  mapper ncores ~chunksize compute [||] al  (fun r -> Array.concat r)
;;


let array_float_parmap ?(ncores=1) ?chunksize (f:'a -> float) (al:'a array) : float array=
  let size = Array.length al in
  let fd = Unix.openfile "/dev/zero" [Unix.O_RDWR; Unix.O_CREAT] 0o600 in
  let arr_out = Bigarray.Array1.map_file fd Bigarray.float64 Bigarray.c_layout true size in
  let compute a lo hi previous exc_handler =
    try 
      for i=lo to hi do Bigarray.Array1.unsafe_set arr_out i (f al.(i)) done
    with e -> exc_handler e lo
  in
  mapper ncores ~chunksize compute () al  (fun r -> ());
  let res = Array.init size (fun i -> Bigarray.Array1.unsafe_get arr_out i) in
  Unix.close fd;
  res
;;  

(*

(*
let array_parmap ?(ncores=1) (f:float -> float) (al:float array) : float array=
 *)
let array_parmap ?(ncores=1) f al =
  (* flush everything *)
  flush stdout; flush stderr;
  (* init task parameters *)
  let ln = Array.length al in
  let chunksize = ln/ncores in
  let fdarr=Array.init ncores (fun _ -> tempfd()) in
  for i = 0 to ncores-1 do
       match Unix.fork() with
      0 -> 
	begin
          let lo=i*chunksize in
          let hi=if i=ncores-1 then ln-1 else (i+1)*chunksize-1 in
	  let res = 
	    try 
	      map_intv lo hi f al;
	    with e -> (Printf.printf "Error: got exception %s\n" (Printexc.to_string e)); raise e
	  in  marshal fdarr.(i) res;
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> ()
  done;
  (* wait for all children *)
  for i = 0 to ncores-1 do try ignore(Unix.wait()) with Unix.Unix_error (Unix.ECHILD, _, _) -> () done;
  (* read in all data *)
  let res = ref [] in
  (* iterate in reverse order, to accumulate in the right order *)
  for i = 0 to ncores-1 do
      res:= ((unmarshal fdarr.((ncores-1)-i)):'d)::!res;
  done;
  (* use extLib's tail recursive one *)
  Array.concat !res
;;

*)

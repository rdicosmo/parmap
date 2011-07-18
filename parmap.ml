(**************************************************************************)
(* ParMap: a simple library to perform Map computations on a multi-core   *)
(*                                                                        *)
(*  Main author(s):  Marco Danelutto, Roberto Di Cosmo                    *)
(*                                                                        *)
(*  This library is free software: you can redistribute it and/or modify  *)
(*  it under the terms of the GNU Lesser General Public License as        *)
(*  published by the Free Software Foundation, either version 3 of the    *)
(*  License, or (at your option) any later version.  A special linking    *)
(*  exception to the GNU Lesser General Public License applies to this    *)
(*  library, see the COPYING file for more information.                   *)
(**************************************************************************)

let mkpipe () =
    let (ifd,ofd) = Unix.pipe () in
    ((fun x -> let s = Marshal.to_string x [Marshal.Closures] in Unix.write ofd s 0 (String.length s)), 
     (fun () -> Marshal.from_channel (Unix.in_channel_of_descr ifd)),
     (fun () -> Printf.printf "Closing out (I am %d)\n" (Unix.getpid()); Unix.close ofd),
     (fun () -> Printf.printf "Closing in (I am %d)\n" (Unix.getpid()); Unix.close ifd)
    )
;;  


let parmap f l ?(ncores=1) =
  (* init task parameters *)
  let ln = List.length l in
  let chunksize = ln/ncores in
  let pidl = ref [] in
  for i = 0 to ncores-1 do
    Printf.printf "Iteration %d\n" i; flush stdout; flush stderr;
    (* create a pipe for writing back the results *)
    let (writep,readp,closew,closer) = mkpipe() in
    match Unix.fork() with
      0 -> 
	begin
	  closer();
          let reschunk=ref [] in
          for j=i*chunksize to (i+1)*chunksize-1 do
	    try 
              reschunk := (f (List.nth l j))::!reschunk
	    with _ -> (Printf.printf "Error: j=%d\n" j)
          done;
          Printf.eprintf "Child: %d finished computing \n" i; 
          Printf.printf "Size: %d\n" (List.length !reschunk);
          List.map (fun n -> Printf.printf "%d\n" n) !reschunk; flush stdout;
          writep (List.rev !reschunk); closew();
          Printf.eprintf "Child: %d exiting in 20 seconds.\n" i; 
          exit 0
	end
    | -1 ->  Printf.eprintf "Fork error: pid %d; i=%d.\n" (Unix.getpid()) i; 
    | pid -> (closew(); Printf.printf "Closed for pid %d\n" pid;pidl:= (pid,readp)::!pidl)
  done;
  let out_of_order_l = 
    List.map 
      (fun _ -> 
	let (pid,s) = Unix.wait() 
	in 
	try 
	  (pid,(List.assoc pid !pidl)())
        with f -> ((Printf.printf "Read error on pid %d\n" pid; (pid,[])); raise f)
      ) !pidl
  in List.flatten (List.map (fun pid -> List.assoc pid out_of_order_l) (List.rev (List.map fst !pidl)))
;;


List.map (fun n -> Printf.printf "%d\n" n) (parmap (fun x -> x+1) [1;2;3;4;5;6;7;8;9;10;11;12] ~ncores:4);;

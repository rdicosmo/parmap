(*****************************************************************************)
(*  Copyright (C) 2009  <pietro.abate@pps.jussieu.fr>                        *)
(*                                                                           *)
(*  This library is free software: you can redistribute it and/or modify     *)
(*  it under the terms of the GNU Lesser General Public License as           *)
(*  published by the Free Software Foundation, either version 3 of the       *)
(*  License, or (at your option) any later version.  A special linking       *)
(*  exception to the GNU Lesser General Public License applies to this       *)
(*  library, see the COPYING file for more information.                      *)
(*****************************************************************************)

(** return a unique identifier *)
let uuid () =
  let rand =
    let s = Random.State.make_self_init () in
    fun () -> Random.State.bits s
  in
  Digest.to_hex (Digest.string (string_of_int (rand ())))

(* This algorithm runs in O(n) . does not preserve ordering - 
   returns elements in reverse order *)
(* XXX it would be nice to add a comparison function here... *)
let list_unique l =
  let seen = Hashtbl.create (2 * (List.length l)) in
  let rec add acc = function
    |hd :: tl when not (Hashtbl.mem seen hd) ->
        begin
          Hashtbl.add seen hd ();
          add (hd :: acc) tl
        end
    |_ :: tl -> add acc tl
    |[] -> acc
  in
  (* List.rev *) add [] l

(* standard memoization function *)
let memo f =
  let h = Hashtbl.create 1023 in
  fun i ->
    try Hashtbl.find h i
    with Not_found -> begin
      let r = f i in
      Hashtbl.add h i r ;
      r
    end

let timestamp () =
  let tm = Unix.localtime (Unix.time ()) in
  Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
    tm.Unix.tm_hour
    tm.Unix.tm_min
    tm.Unix.tm_sec
;;

type label = string

module type Messages = sig
  type t
  val create: ?enabled:bool -> label -> t
  val eprintf: t -> ('a, unit, string, unit) format4 -> 'a
  val enable : label -> unit
  val disable : label -> unit
  val all_disabled : unit -> unit
  val all_enabled : unit -> unit
  val avalaible : unit -> label list
end

(** Debug messages are printed immediately on stderr. 
 * They can enabled or disabled (default) *)
module MakeMessages(X : sig val label : string end) = struct
  type t = {
    label : string;
    mutable enabled : bool
  } 
  let messages = Hashtbl.create 10
  let allenabled = ref false

  let create ?(enabled=false) label =
    if not (Hashtbl.mem messages label) then
      { label = label ; enabled = enabled }
    else begin
      Format.eprintf "The label (%s) %s already exists@." X.label label;
      exit 1
    end

  let eprintf t fmt =
    Printf.kprintf (
      if (t.enabled || !allenabled) then begin
        (fun s -> Format.eprintf "(%s)%s: %s@." X.label t.label s)
      end else ignore
    ) fmt

  let onoff s b =
    try let t = Hashtbl.find messages s in t.enabled <- b
    with Not_found ->
      Printf.eprintf "Warning: debug label %s not found\n" s

  let all_enabled () = allenabled := true
  let all_disabled () = allenabled := false
  let enable s = onoff s true
  let disable s = onoff s false

  let avalaible () = Hashtbl.fold (fun k _ acc -> k::acc) messages []
  let all_enabled () = allenabled := true
  let all_disabled () = allenabled := false
end

(* this way we can have the same label for different messages *)
module Info = MakeMessages(struct let label = "I" end)
module Warning = MakeMessages(struct let label = "W" end)
module Debug = MakeMessages(struct let label = "D" end)

let make_info label =
  let t = Info.create label in
  fun fmt -> Info.eprintf t fmt

let make_warning label =
  let t = Warning.create label in
  fun fmt -> Warning.eprintf t fmt

let make_debug label =
  let t = Debug.create label in
  fun fmt -> Debug.eprintf t fmt

let make_fatal label =
  let l = Printf.sprintf "Fatal error in module %s: " label in
  Printf.kprintf (fun s -> Printf.eprintf "%s%s\n%!" l s; exit (-1))

(** Printf bars are printed immediately on stderr.
 * they can be enabled or disabled (default) *)
module Progress = struct
  type t = {
    name : string ;
    buffer : Buffer.t ;
    mutable total : int ;
    mutable perc : int ;
    mutable rotation : int ;
    mutable enabled : bool ;
  }

  let columns = 75 
  let full = " %100.0\n" 
  let rotate = "|/-\\"
  let bars = Hashtbl.create 10

  let create ?(enabled=false) ?(total=0) s =
    let c = {
      name = s;
      buffer = Buffer.create columns ;
      total = total ;
      perc = 0 ;
      rotation = 0 ;
      enabled = enabled }
    in
    Hashtbl.add bars s c;
    c

  let enable s =
    try let t = Hashtbl.find bars s in t.enabled <- true
    with Not_found ->
      Printf.eprintf "Warning: Progress Bar %s not found\n" s

  let available () = Hashtbl.fold (fun k _ acc -> k::acc) bars []

  let set_total c total = c.total <- total
  let reset c =
    Buffer.clear c.buffer;
    c.perc <- 0;
    c.rotation <- 0

  let progress ?(i=1) c =
    if c.enabled then begin
      c.perc <- c.perc + i;
      Buffer.clear c.buffer;
      Buffer.add_char c.buffer '\r';
      Buffer.add_string c.buffer c.name;
      let f = floor (1000.0 *. (float c.perc) /. (float c.total)) in
      let f = f /. 10.0 in
      if f = 100.0 then Buffer.add_string c.buffer full
      else begin
        c.rotation <- (1 + c.rotation) land 3;
        Printf.bprintf c.buffer "%c %%%4.1f" rotate.[c.rotation] f
      end ;
      Printf.eprintf "%s" (Buffer.contents c.buffer)
    end

end

(** Timers are printed all together when the function dump is called.
 * they can be enabled or disabled (default) *)
module Timer = struct
  type t = {
    name: string;
    mutable total : float;
    mutable last  : float;
    mutable is_in : bool;
    mutable enabled : bool;
  }

  let timers = Hashtbl.create 10
  let gettimeofday = ref (fun _ -> 0.)
  let () = gettimeofday := Unix.gettimeofday

  let pp_timer fmt c =
    Format.fprintf fmt "Timer %s. Total time: %f.@."
      c.name c.total

  let dump fmt () =
    Hashtbl.iter (fun _ c -> if c.enabled then pp_timer fmt c) timers

  let create ?(enabled=false) s =
    let c = { 
      name = s;
      total = 0.;
      last = 0.;
      is_in = false ;
      enabled = enabled } 
    in
    Hashtbl.add timers s c;
    c

  let enable s =
    try let t = Hashtbl.find timers s in t.enabled <- true
    with Not_found ->
      Printf.eprintf "Warning: Timer %s not found\n" s

  let available () = Hashtbl.fold (fun k _ acc -> k::acc) timers []

  let start c =
    assert(not c.is_in);
    c.is_in <- true;
    c.last <- !gettimeofday()

  let stop c x =
    assert(c.is_in);
    c.is_in <- false;
    c.total <- c.total +. (!gettimeofday() -. c.last);
    x
end

let pp_process_time fmt () =
  let pt = Unix.times () in
  Format.fprintf fmt "Process time (user):  %5.2f@." pt.Unix.tms_utime;
  Format.fprintf fmt "Process time (sys):   %5.2f@." pt.Unix.tms_stime
;;

module StringHashtbl = Hashtbl.Make (
  struct
    type t = string
    let equal (a : string) (b : string) = (a = b)
    let hash s = Hashtbl.hash s
  end
)

module IntHashtbl = Hashtbl.Make (
  struct
    type t = int
    let equal (a : int) (b : int) = (a = b)
    let hash i = Hashtbl.hash i
  end
)

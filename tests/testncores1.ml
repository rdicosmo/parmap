(**************************************************************************)
(* Issue #77: when ~ncores=1 (or 0), Parmap should not fork.              *)
(*                                                                        *)
(* The check is indirect but reliable: we use a parent-side ref counter   *)
(* whose increments only persist if the closure runs in the parent. If    *)
(* Parmap forked, the children's increments would be invisible to us.    *)
(**************************************************************************)

open Parmap

let l = [1;2;3;4;5]
let expected_sum = List.fold_left (+) 0 l

let assert_no_fork tag counter =
  if !counter <> List.length l then begin
    Printf.eprintf
      "FAIL [%s]: expected counter=%d (in-process), got %d (fork happened?)\n%!"
      tag (List.length l) !counter;
    exit 1
  end

let () =
  (* parmap, ~ncores:1 *)
  let counter = ref 0 in
  let r = parmap ~ncores:1 (fun x -> incr counter; x*2) (L l) in
  assert (r = List.map (fun x -> x*2) l);
  assert_no_fork "parmap ~ncores:1" counter;

  (* parmap, ~ncores:0 (clamped, also no fork) *)
  let counter = ref 0 in
  let r = parmap ~ncores:0 (fun x -> incr counter; x*2) (L l) in
  assert (r = List.map (fun x -> x*2) l);
  assert_no_fork "parmap ~ncores:0" counter;

  (* parmapi *)
  let counter = ref 0 in
  let r = parmapi ~ncores:1 (fun i x -> incr counter; (i,x)) (L l) in
  assert (r = List.mapi (fun i x -> (i,x)) l);
  assert_no_fork "parmapi ~ncores:1" counter;

  (* pariter *)
  let counter = ref 0 in
  pariter ~ncores:1 (fun _ -> incr counter) (L l);
  assert_no_fork "pariter ~ncores:1" counter;

  (* parfold *)
  let counter = ref 0 in
  let r = parfold ~ncores:1 (fun x acc -> incr counter; x + acc) (L l) 0 (+) in
  assert (r = expected_sum);
  assert_no_fork "parfold ~ncores:1" counter;

  (* parmapfold *)
  let counter = ref 0 in
  let r =
    parmapfold ~ncores:1
      (fun x -> incr counter; x*10)
      (L l)
      (fun mapped acc -> mapped + acc)
      0
      (+)
  in
  assert (r = expected_sum * 10);
  assert_no_fork "parmapfold ~ncores:1" counter;

  (* array_parmap *)
  let counter = ref 0 in
  let r = array_parmap ~ncores:1 (fun x -> incr counter; x+1) [|1;2;3|] in
  assert (r = [|2;3;4|]);
  assert (!counter = 3);

  (* array_float_parmap *)
  let counter = ref 0 in
  let r = array_float_parmap ~ncores:1 (fun x -> incr counter; x +. 0.5) [|1.0;2.0;3.0|] in
  assert (r.(0) = 1.5 && r.(1) = 2.5 && r.(2) = 3.5);
  assert (!counter = 3);

  (* init/finalize must NOT be called when ncores=1 (no child to set up) *)
  let init_called = ref false in
  let finalize_called = ref false in
  let _ =
    parmap
      ~init:(fun _ -> init_called := true)
      ~finalize:(fun () -> finalize_called := true)
      ~ncores:1
      (fun x -> x)
      (L l)
  in
  assert (not !init_called);
  assert (not !finalize_called);

  (* Exceptions raised in user code must propagate to the caller, not exit. *)
  let raised = ref false in
  (try
     ignore (parmap ~ncores:1 (fun _ -> failwith "boom") (L l))
   with Failure _ -> raised := true);
  assert !raised;

  (* get_ncores () should be 1 after a ~ncores:1 call *)
  let _ = parmap ~ncores:1 (fun x -> x) (L l) in
  assert (get_ncores () = 1);

  (* Empty inputs still no-op (pre-existing behavior preserved) *)
  assert (parmap ~ncores:1 (fun x -> x) (L []) = []);
  assert (array_parmap ~ncores:1 (fun x -> x) [||] = [||]);

  print_endline "OK"

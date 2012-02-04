(**************************************************************************)
(* Sample use of ParMap,  a simple library to perform Map computations on *)
(* a multi-core                                                           *)
(*                                                                        *)
(*  Author(s):  Marco Danelutto and Roberto Di Cosmo                      *)
(*                                                                        *)
(*  This program is free software: you can redistribute it and/or modify  *)
(*  it under the terms of the GNU General Public License as               *)
(*  published by the Free Software Foundation, either version 2 of the    *)
(*  License, or (at your option) any later version.                       *)
(**************************************************************************)

(* for the toplevel

#use "topfind";;
#require "graphics";;
#require "parmap";;
#require "sdl";;

*)

let n   = ref 1000;; (* the size of the square screen windows in pixels      *)
let res = ref 1000;; (* the resolution: maximum number of iterations allowed *)

(* scale factor and offset of the picture *)

let scale = ref 1.;;
let ofx = ref 0.;;
let ofy = ref 0.;;

(* convert an integer in the range 0..res into a screen color *)

let color_of c res = Pervasives.truncate 
      (((float c)/.(float res))*.(float Graphics.white));;

(* compute the color of a pixel by iterating z_n+1=z_n^2+c *)
(* j,k are the pixel coordinates                           *)

let pixel (j,k,n) = 
  let zr = ref 0.0 in
  let zi = ref 0.0 in
  let cr = ref 0.0 in
  let ci = ref 0.0 in
  let zrs = ref 0.0 in
  let zis = ref 0.0 in
  let d   = ref (2.0 /. ((float  n) -. 1.0)) in
  let colour = Array.create n (Graphics.black) in

  for s = 0 to (n-1) do
    let j1 = ref (((float  j.(s)) +. !ofx) /. !scale) in
    let k1 = ref (((float  k) +. !ofy) /. !scale) in
    begin
      zr := !j1 *. !d -. 1.0;
      zi := !k1 *. !d -. 1.0;
      cr := !zr;
      ci := !zi;
      zrs := 0.0;
      zis := 0.0;
      for i=0 to (!res-1) do
	begin
	  if(not((!zrs +. !zis) > 4.0))
	  then 
	    begin
	      zrs := !zr *. !zr;
	      zis := !zi *. !zi;
	      zi  := 2.0 *. !zr *. !zi +. !ci;
	      zr  := !zrs -. !zis +. !cr;
	      Array.set colour s (color_of i !res);
	    end;
    	end
      done
    end
  done;
  (colour,k);;

(* generate the initial configuration *)

let initsegm n = 
  let rec aux acc = function 0 -> acc | n -> aux (n::acc) (n-1) in
  aux [] n
;;

let tasks = 
  let ini = Array.create !n 0 in
  let iniv = 
    for i=0 to (!n-1) do
      Array.set ini i i
    done; ini in
  List.map (fun seed -> (iniv,seed,!n)) (initsegm !n)
;;

(* draw a line on the screen using fast image functions *)

let unpack_color n = 
  let r = (n land 0xff0000) lsr 16 and g = (n land 0x00ff00) lsr 8 and b = n land 0xff in
  (r,g,b)
;;

let draw_line screen (col,j) =
  Array.iteri 
    (fun i c -> Sdlvideo.put_pixel_color screen i j (unpack_color c))
    col;;

let draw screen res = List.iter (fun c -> draw_line screen c) res;;

(* compute *)

let compute () = Parmap.parmap ~ncores:4 ~chunksize:1 pixel (Parmap.L tasks);;

(*** Open the main graphics window and run the event loop *)

Sdl.init [`VIDEO];;

let (bpp, w, h) = (24, !n, !n);;
let screen = Sdlvideo.set_video_mode ~w ~h ~bpp [];;

(* a shadow surface for saving image areas *)

let shadowbuf = Sdlvideo.create_RGB_surface_format screen [`SWSURFACE] ~w ~h;;

(* draw h/v lines and a rectangle *)

let draw_hline x y l col = for i = x to x+l do Sdlvideo.put_pixel_color ~x:i ~y screen col done;;
let draw_vline x y l col = for j = y to y+l do Sdlvideo.put_pixel_color ~x ~y:j screen col done;;
let draw_rect x y w h col =
  draw_hline x y w col; draw_hline x (y+h) w col; draw_vline x y h col; draw_vline (x+w) y h col;;

(* draw *)

let redraw () = 
  Printf.eprintf "Computing...%!";
  draw screen (compute()); 
  Sdlvideo.update_rect screen;
  Printf.eprintf "done.\n%!";;

(* event loop for zooming into the picture *)

let rezoom x y w =
   let deltas = ((float !n)/.(float w)) in
   ofx := (!ofx +. (float x)) *. deltas;
   ofy := (!ofy +. (float y)) *. deltas;
   scale := !scale *. deltas;
   redraw();;
let reset () = scale:=1.; ofx:=0.; ofy:=0.;redraw();;
let refine () = res:=!res*2; redraw ();;
let unrefine () = res:=!res/2; redraw ();;
let zoom_in () = rezoom (!n/4) (!n/4) (!n/2);;
let zoom_out () = rezoom (-(!n/2)) (-(!n/2)) (!n*2);;
let dumpnum = ref 0;;
let dump () = 
  Printf.eprintf "Dumping image taken at ofx: %f ofy: %f scale: %f\n%!" !ofx !ofy !scale;
  Sdlvideo.save_BMP screen (Printf.sprintf "mandels-ofx-%f-ofy-%f-scale-%f.bmp" !ofx !ofy !scale);;

(* encode state machine here *)

open Sdlevent;;
open Sdlkey;;
open Sdlvideo;;

let rec init () =
  match wait_event () with
  |  KEYDOWN {keysym=k} -> 
    (match k with
      KEY_PLUS -> let _ = zoom_in() in init ()
    | KEY_MINUS -> let _ = zoom_out() in init ()
    | KEY_r -> let _ = refine () in init ()
    | KEY_u -> let _ = unrefine () in init ()
    | KEY_c -> let _ = reset() in init ()
    | KEY_d -> let _ = dump() in init ()
    | KEY_q -> Sdl.quit
    | _ -> init ()
    )
  | MOUSEBUTTONDOWN {mbe_x=x;mbe_y=y} -> track_rect x y (x,y,0,0)
  | _ -> init ()

and track_rect x y (ox,oy,ow,oh) =
  match wait_event () with
  | MOUSEBUTTONUP {mbe_x=x';mbe_y=y'} -> 
    let bx,by,w,h = (min x x'), (min y y'), (abs (x'-x)), (abs (y'-y)) in
    (rezoom bx by (min w h); init())
  | MOUSEMOTION  {mme_x=x';mme_y=y'} -> 
    let bx,by,w,h = (min x x'), (min y y'), (abs (x'-x)), (abs (y'-y)) in
      (* restore old image if necessary *)
      if ow>0 & oh>0 then begin
	let oldrect = {r_x=ox;r_y=oy;r_w=ow;r_h=oh} in
	blit_surface ~src:shadowbuf ~src_rect:oldrect ~dst:screen ~dst_rect:oldrect ();
        Sdlvideo.update_rect ~rect:oldrect screen;
      end;
      (* save image if necessary *)
      if w>0 & h>0 then begin
	let newrect = {r_x=bx;r_y=by;r_w=w;r_h=h} in
	blit_surface ~src:screen ~src_rect:newrect ~dst:shadowbuf ~dst_rect:newrect ();
	(* draw the border _inside_ the area *)
	draw_rect bx by (w-1) (h-1) Sdlvideo.white;
        Sdlvideo.update_rect ~rect:newrect screen;
      end;
    track_rect x y (bx,by,w,h)
| _ -> track_rect x y (ox,oy,ow,oh)
;;

redraw();;
init()


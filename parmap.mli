module type MemoryMarshal =
  sig
    val unmarshal : Unix.file_descr -> 'a
    val marshal : int -> Unix.file_descr -> 'a -> unit
  end
module MmapBigArray : MemoryMarshal
module MmapXenTools : MemoryMarshal
module Parmap :
  functor (M : MemoryMarshal) ->
    sig
      val t : Util.Timer.t
      val tc : Util.Timer.t
      val tm : Util.Timer.t
      val tu : Util.Timer.t
      val tempfd : unit -> Unix.file_descr
      val parmap : ('a -> 'b) -> 'a list -> ?ncores:int -> 'b list
    end
module PmapBA :
  sig
    val t : Util.Timer.t
    val tc : Util.Timer.t
    val tm : Util.Timer.t
    val tu : Util.Timer.t
    val tempfd : unit -> Unix.file_descr
    val parmap : ('a -> 'b) -> 'a list -> ?ncores:int -> 'b list
  end
module PmapXT :
  sig
    val t : Util.Timer.t
    val tc : Util.Timer.t
    val tm : Util.Timer.t
    val tu : Util.Timer.t
    val tempfd : unit -> Unix.file_descr
    val parmap : ('a -> 'b) -> 'a list -> ?ncores:int -> 'b list
  end
val parmap : ('a -> 'b) -> 'a list -> ?ncores:int -> 'b list

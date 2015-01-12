open Batteries

(* should be set on the MDS side and read-only for DSs *)
let chunk_size = ref (1 * 1024 * 1024)

type node = { host: string ;
              port: int    }

let create_node host port =
  { host ; port }

type chunk = { rank: int    ;
               data: string }

module Chunk = struct
  type t = chunk
  let compare c1 c2 =
    Int.compare c1.rank c2.rank
end

type file = { name  : string            ;
              chunks: Set.Make(Chunk).t }

module File = struct
  type t = file
  let compare f1 f2 =
    String.compare f1.name f2.name
end

type files = Set.Make(File).t

type msg_type = Raw | Compressed | Signed | Encrypted

type answer = Ok | Error of string

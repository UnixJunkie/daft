open Batteries
open Printf

module FU = FileUtil

(* FBR: encapsulate those into separate modules *)

(* should be set on the MDS side and read-only for DSs *)
let chunk_size = ref (1 * 1024 * 1024)

type node = { host: string ;
              port: int    }

let create_node host port =
  { host ; port }

let string_of_node n =
  sprintf "%s:%d" n.host n.port

type chunk = { rank: int    ;
               data: string }

module Chunk = struct
  type t = chunk
  let compare c1 c2 =
    BatInt.compare c1.rank c2.rank
end

type managed_file = { name: string ;
                      size: int64 ;
                      stat: FU.stat ;
                      nb_chunks: int ;
                      (* (Some x) if x < chunk_size *)
                      last_chunk_size: int64 option }

let create_managed_file name size stat nb_chunks last_chunk_size =
  { name; size; stat; nb_chunks; last_chunk_size }

module File = struct
  type t = managed_file
  let compare f1 f2 =
    String.compare f1.name f2.name
end

let dummy_stat = FU.stat "/dev/null"

module FileSet = struct (* extend type with more operations *)
  include Set.Make(File)
  let contains_fn fn s =
    let dummy = create_managed_file fn Int64.zero dummy_stat 0 None in
    mem dummy s
end

type files = Set.Make(File).t

type msg_type = Raw | Compressed | Signed | Encrypted

type answer = Ok | Error of string

open Batteries

let chunk_size = 64 * 1024 * 1024

type server = { host: string ;
                port: int    }

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

type node = Data_node of server | Meta_data_node of server

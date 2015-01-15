open Batteries
open Printf

module FU = FileUtil

module Node = struct
  type t = { host: string ; port: int }
  let create host port =
    { host ; port }
  let to_string n =
    sprintf "%s:%d" n.host n.port
end

module File = struct

  module Chunk = struct
    let default_size = 1024 * 1024
    type t = { rank: int ;
               size: int64 option (* None if default_size; (Some x) else *)
               (* FBR: it has a list/array of nodes too *) }
    let create rank size =
      { rank ; size }
    let compare c1 c2 =
      BatInt.compare c1.rank c2.rank
  end

  module ChunkSet = struct
    include Set.Make(Chunk)
    let create nb_chunks last_chunk_size =
      assert(nb_chunks > 0); (* DAFT is not for files of null size *)
      let rec loop acc i =
        if i = nb_chunks - 1 then
          add (Chunk.create i last_chunk_size) acc
        else
          let new_acc = add (Chunk.create i None) acc in
          loop new_acc (i + 1)
      in
      loop empty 0
  end

  type t = { name: string ;
             size: int64 ;
             stat: FU.stat ;
             nb_chunks: int ;
             chunks: ChunkSet.t }
  let create name size stat nb_chunks last_chunk_size =
    let chunks = ChunkSet.create nb_chunks last_chunk_size in
    { name; size; stat; nb_chunks ; chunks }
  let compare f1 f2 =
    String.compare f1.name f2.name

end

(* the status of the "filesystem" is just a set of files *)
module FileSet = struct
  include Set.Make(File)
  let dummy_stat = FU.stat "/dev/null" (* should be private *)
  (* extend module with more operations *)
  let contains_fn fn s =
    let dummy_file = File.({ name = fn;
                             size = Int64.zero;
                             stat = dummy_stat;
                             nb_chunks = 0;
                             chunks = ChunkSet.empty })
    in
    mem dummy_file s
end

type storage_mode = Raw | Compressed | Signed | Encrypted

type answer = Ok | Error of string

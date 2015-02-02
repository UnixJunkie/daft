open Batteries
open Printf

module FU = FileUtil

module Node = struct
  (* the rank allows to uniquely identify a node; a la MPI *)
  type t = { rank: int    ;
             host: string ;
             port: int    }
  let create rank host port =
    { rank; host; port }
  let dummy () =
    { rank = -1; host = "dummy_hostname"; port = -1 }
  let to_string n =
    sprintf "%d.%s:%d" n.rank n.host n.port
  let of_string s =
    Scanf.sscanf s "%d.%s:%d" create
  let compare n1 n2 =
    BatInt.compare n1.rank n2.rank
end

module NodeSet = struct
  include Set.Make(Node)
end

module File = struct

  module Chunk = struct
    let default_size = 1024 * 1024
    type t = { rank:  int          ;
               size:  int64 option ; (* None if default_size; (Some x) else *)
               nodes: NodeSet.t    } (* which nodes have this chunk
                                        in their datastore *)
    let create rank size node =
      let nodes = NodeSet.singleton node in
      { rank; size; nodes }
    let compare c1 c2 =
      BatInt.compare c1.rank c2.rank
  end

  module ChunkSet = struct
    include Set.Make(Chunk)
    let create nb_chunks last_chunk_size node =
      assert(nb_chunks > 0); (* DAFT is not for files of null size *)
      let rec loop acc i =
        if i = nb_chunks - 1 then
          add (Chunk.create i last_chunk_size node) acc
        else
          let new_acc = add (Chunk.create i None node) acc in
          loop new_acc (i + 1)
      in
      loop empty 0
  end

  type t = { name:      string     ;
             size:      int64      ;
             stat:      FU.stat    ;
             nb_chunks: int        ;
             chunks:    ChunkSet.t }
  let create name size stat nb_chunks last_chunk_size node =
    let chunks = ChunkSet.create nb_chunks last_chunk_size node in
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

(* FBR: organize this into modules?
        - add encode/decode for string conversion *)

(* the MDS is a master, DSs are its slaves *)

type ds_to_mds_message =
  | Join of Node.t (* a DS registering itself with the MDS *)
  | Got_chunk of string * int (* ACK a chunk (filename, chunk_number) *)

type mds_to_ds_message =
  | Send_to of int * string * int (* send order (receiver_ds_rank, filename, chunk_number) *)
  | Quit (* DS should exit *)

type ds_to_ds_message =
  | Chunk of string * int * string (* file chunk (filename, chunk_number, string) *)

type cli_to_mds_message =
  | Add_file of File.t
  | Quit (* MDS must send Quit to all DSs then exit itself *)

type cli_to_ds_message =
  | Add_file of File.t

type mds_to_cli_message =
  | Already_there of string (* filename *)

type for_MDS_message =
  | From_DS of ds_to_mds_message
  | From_CLI of cli_to_mds_message

type for_DS_message =
  | From_DS of ds_to_ds_message
  | From_CLI of cli_to_ds_message

type for_CLI_message =
  | From_MDS of mds_to_cli_message

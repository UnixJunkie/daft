open Batteries
open Printf

exception Loop_end

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
  let get_rank n =
    n.rank
  let get_host n =
    n.host
  let get_port n =
    n.port
  let to_string n =
    sprintf "%d.%s:%d" n.rank n.host n.port
  let to_triplet n =
    (n.rank, n.host, n.port)
  let of_string s =
    Scanf.sscanf s "%d.%s:%d" create
  let compare n1 n2 =
    BatInt.compare n1.rank n2.rank
end

module NodeSet = struct
  include Set.Make(Node)
  let to_string ns =
    let res = Buffer.create 1024 in
    Buffer.add_string res "[";
    iter (fun n ->
        Buffer.add_string res (Node.to_string n);
        Buffer.add_string res "; "
      ) ns;
    Buffer.add_string res "]";
    Buffer.contents res
end

module RNG = Random
let _ = RNG.self_init ()

module File = struct
  module Chunk = struct
    type t = { rank:  int          ;
               size:  int64 option ; (* None if default_size; (Some x) else *)
               nodes: NodeSet.t    } (* which nodes have this chunk
                                        in their datastore *)
    let create rank size node =
      let nodes = NodeSet.singleton node in
      { rank; size; nodes }
    let compare c1 c2 =
      BatInt.compare c1.rank c2.rank
    let to_string c =
      let string_of_size = function
        | None -> ""
        | Some s -> sprintf "size: %Ld " s
      in
      sprintf "rank: %d %snodes: %s"
        c.rank (string_of_size c.size) (NodeSet.to_string c.nodes)
    exception Found of Node.t
    (* randomly select a DS having this chunk *)
    let select_source_rand (c: t): Node.t =
      let n = NodeSet.cardinal c.nodes in
      let rand = RNG.int n in
      let i = ref 0 in
      try
        NodeSet.iter (fun node ->
            if !i = rand then raise (Found node)
            else incr i
          ) c.nodes;
        assert(false)
      with Found node -> node
  end

  module ChunkSet = struct
    include Set.Make(Chunk)
    let create nb_chunks last_chunk_size node =
      let rec loop acc i =
        if i = nb_chunks - 1 then
          add (Chunk.create i last_chunk_size node) acc
        else
          let new_acc = add (Chunk.create i None node) acc in
          loop new_acc (i + 1)
      in
      if nb_chunks <= 0 then empty
      else loop empty 0
    let to_string cs =
      let res = Buffer.create 1024 in
      iter (fun c -> Buffer.add_string res (Chunk.to_string c)
           ) cs;
      Buffer.contents res
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
  let to_string f =
    sprintf "name: %s size: %Ld #chunks: %d\n%s"
      f.name f.size f.nb_chunks (ChunkSet.to_string f.chunks)
end

(* the status of the "filesystem" is just a set of files *)
module FileSet = struct
  include Set.Make(File)
  let dummy_stat = FU.stat "/dev/null" (* should be private *)
  (* extend module with more operations *)
  let dummy_file fn =
    File.({ name = fn;
            size = Int64.zero;
            stat = dummy_stat;
            nb_chunks = 0;
            chunks = ChunkSet.empty })
  let contains_fn fn s =
    mem (dummy_file fn) s
  let find_fn fn s =
    find (dummy_file fn) s
  let remove_fn fn s =
    remove (dummy_file fn) s
  let to_string fs =
    let res = Buffer.create 1024 in
    iter (fun f ->
        Buffer.add_string res (File.to_string f);
        Buffer.add_string res "\n"
      ) fs;
    Buffer.contents res
end

(* only support Raw mode until all commands are properly working
   Compressed mode will be the next priority
   Compressed, Signed and Encrypted can be combined so there is
   a total of eight modes
   signing alone could provide some sort of authentication for
   commands which allow to control the state of the system; an
   attacker could observe the system if he is on the same network
   but not control it. *)
type data_mode = Raw | Compressed | Encrypted | Signed

module Protocol = struct

  (* messages naming
     ---------------
     *_req: request that will need an answer
     *_ack: positive answer to a request
     *_nack: negative answer to a request
     *_push: message that doesn't need an answer or whose answer will not
             be sent by the one receiving it
     *_cmd_*: related to a command from the CLI *)

  (* we might need a cli_rank in the future, and assume there are at most
     as many CLIs as there are DSs *)
  type ds_rank = int
  type filename = string
  type chunk_id = int
  type chunk_data = string

  type ds_to_mds =
    | Join_push of Node.t (* a DS registering itself with the MDS *)
    | Chunk_ack of filename * chunk_id
    | Add_file_req of ds_rank * File.t

  type mds_to_ds =
    | Add_file_ack of filename
    | Add_file_nack of filename
    | Send_to_req of ds_rank * filename * chunk_id
    | Quit_cmd

  type ds_to_ds =
    | Chunk of filename * chunk_id * chunk_data

  type cli_to_mds =
    | Ls_cmd_req
    | Fetch_cmd_req of ds_rank * filename
    | Quit_cmd

  type mds_to_cli =
    | Ls_cmd_ack of FileSet.t
    | Fetch_cmd_nack of filename

  type file_loc = Local (* disk *) | Remote (* host *)

  type cli_to_ds =
    | Fetch_file_cmd_req of filename * file_loc

  type fetch_error = Already_here | Is_directory | Copy_failed | No_such_file

  type ds_to_cli =
    | Fetch_file_cmd_ack of filename
    | Fetch_file_cmd_nack of filename * fetch_error

  type t = DS_to_MDS of ds_to_mds
         | MDS_to_DS of mds_to_ds
         | DS_to_DS of ds_to_ds
         | CLI_to_MDS of cli_to_mds
         | MDS_to_CLI of mds_to_cli
         | CLI_to_DS of cli_to_ds
         | DS_to_CLI of ds_to_cli

  (* FBR: add to_string *)

  (* FBR: add mode parameter *)
  let encode (m: t): string =
    Marshal.to_string m [Marshal.No_sharing]

  (* FBR: add mode parameter *)
  let decode (s: string): t =
    (* we should check the message is valid before unmarshalling it
       (or software can crash);
       maybe not when in Raw mode but other modes should do it *)
    (Marshal.from_string s 0: t)

  let string_of_fetch_error = function
    | Already_here -> "already here"
    | Is_directory -> "directory"
    | Copy_failed -> "copy failed"
    | No_such_file -> "no such file"

end

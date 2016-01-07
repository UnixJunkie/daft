open Batteries
open Legacy.Printf

exception Loop_end

module FU = FileUtil
module IntMap = Map.Make(BatInt)
module IntSet = Set.Make(BatInt)

(* some type aliases to make signatures more readable *)
type chunk_data = string
type chunk_id = int
type rank = int
type filename = string
type hostname = string
type is_last = bool
type port = int
type root_node = int
type step_number = int

(* module useful to transform a set of integers with holes
   into a set of intervals *)
module Interval = struct
  type t = { lbound: int ;
             rbound: int }
  let create (lbound: int) (rbound: int): t =
    assert (lbound <= rbound);
    { lbound ; rbound }
  let degenerate (x: int): t =
    { lbound = x ; rbound = x }
  (* compare lbounds *)
  let compare (i1: t) (i2: t): int =
    Int.compare i1.lbound i2.lbound
  (* try to augment the right bound *)
  let may_extend_right (itv: t) (i: int): t option =
    if itv.rbound = i then
      Some itv
    else if itv.rbound + 1 = i then
      Some { itv with rbound = i }
    else
      None
  (* compact string representation *)
  let to_string { lbound ; rbound } =
    if lbound = rbound then
      sprintf "[%d]" lbound
    else
      sprintf "[%d..%d]" lbound rbound
end

module IntervalSet = Set.Make(Interval)
module StringSet = Set.Make(String)

module Node = struct
  (* the rank allows to uniquely identify a node; a la MPI *)
  type t = { rank:     rank        ;
             host:     hostname    ;
             ds_port:  port        ;
             cli_port: port option }
  let create rank host ds_port cli_port =
    { rank; host; ds_port; cli_port }
  let dummy () =
    { rank = -1; host = "!!!dummy_hostname!!!"; ds_port = -1; cli_port = None }
  let dummy_with_rank rank =
    { rank; host = "!!!dummy_hostname!!!"; ds_port = -1; cli_port = None }
  let get_rank n =
    n.rank
  let get_host n =
    n.host
  let get_port n =
    n.ds_port
  let get_cli_port n =
    n.cli_port
  let to_string n =
    sprintf "%d.%s:%d:%s" n.rank n.host n.ds_port
      (match n.cli_port with
       | None -> "None"
       | Some p -> sprintf "(Some %d)" p)
  (* unique identifier for this node; ignores the optional cli_port *)
  let to_string_id n =
    sprintf "%d.%s:%d" n.rank n.host n.ds_port
  let to_quad n =
    (n.rank, n.host, n.ds_port, n.cli_port)
  let compare n1 n2 =
    BatInt.compare n1.rank n2.rank
end

(* create fresh nonces and check their freshness *)
module Nonce_store = struct
  let nonces = ref StringSet.empty
  let fresh (counter: int ref) (n: Node.t): string =
    let nonce =
      sprintf "%s:%d:%d" (Node.get_host n) (Node.get_port n) !counter
    in
    if !counter = -1 then failwith "fresh: counter has looped";
    incr counter;
    nonces := StringSet.add nonce !nonces;
    nonce
  let is_fresh (nonce: string): bool =
    if StringSet.mem nonce !nonces then
      false
    else begin
      nonces := StringSet.add nonce !nonces;
      true
    end
end

module NodeSet = struct
  include Set.Make(Node)
  let to_interval_set ns =
    let init = (None, IntervalSet.empty) in
    let last, glob =
      fold (fun node (loc_acc, glob_acc) ->
          let curr = Node.get_rank node in
          match loc_acc with
          | None -> (Some (Interval.degenerate curr), glob_acc)
          | Some x ->
            match Interval.may_extend_right x curr with
            | None -> (Some (Interval.degenerate curr), IntervalSet.add x glob_acc)
            | Some new_acc -> (Some new_acc, glob_acc)
        ) ns init
    in
    match last with
    | None -> glob
    | Some x -> IntervalSet.add x glob
  let to_string ns =
    let res = Buffer.create 1024 in
    let intervals = to_interval_set ns in
    IntervalSet.iter (fun s ->
        Buffer.add_string res (Interval.to_string s)
      ) intervals;
    Buffer.contents res
end

module RNG = Random
let _ = RNG.self_init ()

module File = struct
  module Chunk = struct
    type t = { id:    chunk_id     ;
               size:  int64 option ; (* None if default_size; (Some x) else *)
               nodes: NodeSet.t    } (* which nodes have this chunk
                                        in their datastore *)
    let create (id: chunk_id) (size: int64 option) (node: Node.t): t =
      let nodes = NodeSet.singleton node in
      { id; size; nodes }
    let dummy (id: chunk_id): t =
      { id; size = None; nodes = NodeSet.empty }
    let get_id (c: t): chunk_id =
      c.id
    let get_size (c: t): int64 option =
      c.size
    let compare (c1: t) (c2: t): int =
      BatInt.compare c1.id c2.id
    let to_string (c: t): string =
      let string_of_size = function
        | None -> ""
        | Some s -> sprintf " size: %Ld" s
      in
      sprintf "cid: %d nodes: %s%s"
        c.id (NodeSet.to_string c.nodes) (string_of_size c.size)
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
    (* a new node possess this chunk *)
    let add_source (c: t) (n: Node.t): t =
      let prev_set = c.nodes in
      let new_set = NodeSet.add n c.nodes in
      if prev_set == new_set then
        Log.error "add_source: already known chunk_id: %d node: %s"
          c.id (Node.to_string n)
      ;
      { c with nodes = new_set }
    (* is this chunk on the node with that nid? *)
    let has_source_nid (c: t) (nid: rank): bool =
      NodeSet.mem (Node.dummy_with_rank nid) c.nodes
  end

  module ChunkSet = struct
    include Set.Make(Chunk)
    let create
        (nb_chunks: int)
        (last_chunk_size: int64 option)
        (node: Node.t) =
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
      let first_time = ref true in
      iter (fun c ->
          if !first_time then first_time := false
          else Buffer.add_char res '\n';
          Buffer.add_char res ' '; (* some indentation *)
          Buffer.add_string res (Chunk.to_string c)
        ) cs;
      Buffer.contents res
    let find_id (id: chunk_id) (cs: t): Chunk.t =
      find (Chunk.dummy id) cs
  end

  type t = { name:       filename   ;
             size:       int64      ;
             creat_time: float      ; (* when file was added *)
             final_time: float      ; (* when file was finalized *)
             nb_chunks:  int        ; (* known when the file is complete *)
             chunks:     ChunkSet.t } (* chunks currently available *)
  (* complete chunkset of file f *)
  let all_chunks
      (nb_chunks: int)
      (last_chunk_size: int64 option)
      (node: Node.t): ChunkSet.t =
    ChunkSet.create nb_chunks last_chunk_size node
  let create
      (name: filename)
      (size: int64)
      (creat_time: float)
      (final_time: float)
      (nb_chunks: int)
      (chunks: ChunkSet.t) =
    { name; size; creat_time; final_time; nb_chunks; chunks }
  (* incoming chunk *)
  let add_chunk (f: t) (c: Chunk.t) (now: float) =
    let prev_set = f.chunks in
    let new_set = ChunkSet.add c prev_set in
    if prev_set == new_set then
      Log.error "duplicate chunk: f: %s chunk: %d" f.name Chunk.(c.id)
    ;
    { f with chunks = new_set ;
             final_time = now }
  let update_chunk (f: t) (c: Chunk.t) (now: float): t =
    { f with
      chunks = ChunkSet.update c c f.chunks ;
      final_time = now }
  (* to list files without chunk details *)
  let forget_chunks (f: t): t =
    { f with chunks = ChunkSet.empty }
  (* check if chunk is already present *)
  let has_chunk (f: t) (cid: chunk_id) =
    let chunk = Chunk.dummy cid in
    ChunkSet.mem chunk f.chunks
  let compare f1 f2 =
    String.compare f1.name f2.name
  let to_string f =
    sprintf "fn: %s size: %Ld #chunks: %d%s"
      f.name f.size f.nb_chunks
      (if f.chunks = ChunkSet.empty then ""
       else ("\n" ^ ChunkSet.to_string f.chunks))
  let get_chunks (f: t): ChunkSet.t =
    f.chunks
  let find_chunk_id (id: chunk_id) (f: t): Chunk.t =
    ChunkSet.find_id id f.chunks
  let get_nb_chunks (f: t): int =
    f.nb_chunks
  let is_last_chunk (c: Chunk.t) (f: t): bool =
    Chunk.(match c.size with
        | Some _s -> true
        | None -> (c.id = f.nb_chunks - 1)
      )
  (* check if 'f' is in the given 'dir' *)
  let is_in_dir (f: t) (dir: string): bool =
    String.starts_with f.name dir
end

(* to be unambiguous, a directory name must end with a '/' *)
let prepare_dirname dir = 
  if String.ends_with dir "/" then dir
  else dir ^ "/"

(* the status of the "filesystem" is just a set of files *)
module FileSet = struct
  include Set.Make(File)
  let add f fs =
    assert(not (mem f fs)); (* it should not be there already *)
    add f fs
  (* extend module with more operations *)
  let dummy_file fn =
    File.({ name = fn;
            size = Int64.zero;
            creat_time = 0.0;
            final_time = 0.0;
            nb_chunks = 0;
            chunks = ChunkSet.empty })
  let contains_fn fn s =
    mem (dummy_file fn) s
  let contains_dir dir s =
    let dn = prepare_dirname dir in
    exists (fun f -> File.is_in_dir f dn) s
  (* return the set of files of the given directory;
     it is empty in case the directory does not exist *)
  let find_dir dirname s =
    let dn = prepare_dirname dirname in
    let dir_files, _others = partition (fun f -> File.is_in_dir f dn) s in
    dir_files
  let find_fn fn s =
    find (dummy_file fn) s
  let remove_fn fn s =
    remove (dummy_file fn) s
  let forget_chunks s =
    fold (fun f acc ->
        add (File.forget_chunks f) acc
      ) s empty
  let to_string fs =
    let res = Buffer.create 1024 in
    let first_time = ref true in
    iter (fun f ->
        if !first_time then first_time := false
        else Buffer.add_char res '\n';
        Buffer.add_string res (File.to_string f)
      ) fs;
    Buffer.contents res
end

module Protocol = struct

  (* messages naming
     ---------------
     *_req: request that will need an answer
     *_ack: positive answer to a request
     *_nack: negative answer to a request
     *_push: message that doesn't need an answer or whose answer will not
             be sent by the actor receiving the message
     *_cmd_*: related to a command from the CLI *)

  type bcast_plan =
    int list IntMap.t (* keys: source nodes
                         values: list of destination nodes once the
                                 corresponding key/source has been filled *)
  type bcast_method = Chain | Binary | Binomial

  type ds_to_mds =
    | Join_push of Node.t (* a DS registering itself with the MDS *)
    | Chunk_ack of filename * chunk_id * rank
    | Add_file_req of rank * File.t
    | Bcast_file_req of rank * File.t * bcast_method
    | Fetch_file_req of rank * filename

  type mds_to_ds =
    | Add_file_ack of filename
    | Add_file_nack of filename
    | Send_to_req of rank * filename * chunk_id * is_last
    | Quit_cmd

  type ds_to_ds =
    | Chunk of
        filename * chunk_id * is_last * chunk_data
    | Bcast_chunk_binary of
        filename * chunk_id * is_last * chunk_data * root_node * step_number
    | Bcast_chunk_binomial of
        filename * chunk_id * is_last * chunk_data * bcast_plan
    | Relay_chunk of
        filename * chunk_id * is_last * chunk_data * root_node

  type cli_to_mds =
    | Ls_cmd_req of rank * bool * filename option
    | Quit_cmd
    | Connect_push of rank * port (* a CLI local to DS of rank 'rank' and
                                     listening on 'port' has connected *)

  type mds_to_cli =
    | Ls_cmd_ack of FileSet.t * string
    | Fetch_cmd_nack of filename
    | Unlock (* hack for experiments *)

  type file_loc = Local (* local disk *) | Remote (* remote host *)

  type cli_to_ds =
    | Fetch_file_cmd_req of filename * file_loc
    | Extract_file_cmd_req of filename * filename
    | Bcast_file_cmd_req of filename * bcast_method
    | Connect_cmd_push of port (* CLI listening port on the same host
                                  than the DS receiving the command *)

  type error = Already_here | Is_directory | Copy_failed | No_such_file

  type ds_to_cli =
    | Fetch_file_cmd_ack of filename
    | Fetch_file_cmd_nack of filename * error
    | Bcast_file_ack

  (* restrictive views of all possible messages *)
  type to_cli =
    | MDS_to_CLI of mds_to_cli
    |  DS_to_CLI of  ds_to_cli
  type from_cli =
    | CLI_to_MDS of cli_to_mds
    | CLI_to_DS  of cli_to_ds
  type to_ds =
    | MDS_to_DS of mds_to_ds
    |  DS_to_DS of  ds_to_ds
    | CLI_to_DS of cli_to_ds
  type from_ds =
    | DS_to_MDS of ds_to_mds
    | DS_to_DS  of ds_to_ds
    | DS_to_CLI of ds_to_cli
  type to_mds =
    |  DS_to_MDS of  ds_to_mds
    | CLI_to_MDS of cli_to_mds
  type from_mds =
    | MDS_to_DS  of mds_to_ds
    | MDS_to_CLI of mds_to_cli

  let string_of_error = function
    | Already_here -> "already here"
    | Is_directory -> "directory"
    | Copy_failed -> "copy failed"
    | No_such_file -> "no such file"

end

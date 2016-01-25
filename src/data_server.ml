open Batteries
open Legacy.Printf
open Types.Protocol

module Fn = Filename
module FU = FileUtil
module IntMap = Types.IntMap
module IntSet = Types.IntSet
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet
module Chunk = File.Chunk
module ChunkSet = File.ChunkSet

let send, receive = Socket_wrapper.DS_socket.(send, receive)
let send_as_is = Socket_wrapper.DS_socket.send_as_is

let msg_counter = ref 0
let rng = Utils.create_CSPRNG ()

module Binary = struct
  (* broadcasting technique:
     compute two children per node and stop once we are looping *)
  let incr_mod i nb_nodes =
    (i + 1) mod nb_nodes
  (* when you receive a chunk to broadcast: you call fork to know
     if and to who you need to forward those chunks *)
  let fork root_rank step_num my_rank nb_nodes =
    assert(nb_nodes > 1); (* enforce distributed setting *)
    let step_num' = step_num + 1 in
    if nb_nodes = 2 then (* special case *)
      if step_num = 0 then (* send to only other node *)
        let next_rank = incr_mod my_rank nb_nodes in
        ([next_rank], step_num')
      else (* stop *)
        ([], step_num')
    else
      let i = (my_rank + step_num + 1) mod nb_nodes in
      let j = incr_mod i nb_nodes in
      if i = root_rank || j = my_rank then
        ([], step_num') (* stop *)
      else if j = root_rank then
        ([i], step_num') (* last send *)
      else
        ([i; j], step_num') (* generic case *)
  (* ranks is for tests in the toplevel only *)
  let rec ranks root_rank step_num my_rank nb_nodes acc =
    match fork root_rank step_num my_rank nb_nodes with
    | [], _ -> List.rev acc
    | [i], _ -> List.rev ((my_rank, [i]) :: acc)
    | [i; j], next_step ->
      let next_rank = incr_mod my_rank nb_nodes in
      ranks root_rank next_step next_rank nb_nodes ((my_rank, [i; j]) :: acc)
    | _ -> assert(false)
end

(* maximize the number of data sources at each step of the broadcast *)
module Binomial = struct
  let plan (src_node: int) (nb_nodes: int): bcast_plan =
    (* IntSet union of all keys and all values lists *)
    let bindings_union m =
      let res =
        IntMap.fold (fun k v acc1 ->
            let acc2 = IntSet.add k acc1 in
            List.fold_left (fun acc3 x ->
                IntSet.add x acc3
              ) acc2 v
          ) m IntSet.empty
      in
      IntSet.elements res
    in
    assert(nb_nodes > 1);
    let all_nodes = List.range 0 `To (nb_nodes - 1) in
    let dst_nodes = List.filter ((<>) src_node) all_nodes in
    let src_nodes = [src_node] in
    let rec loop acc sources destinations = match sources, destinations with
      | (_, []) -> acc
      | ([], _) ->
        if acc = IntMap.empty then
          failwith "compute_plan: no sources but some destinations"
        else
          loop acc (bindings_union acc) destinations
      | (src :: other_src, dst :: other_dst) ->
        let new_acc =
          try
            let prev = IntMap.find src acc in
            let curr = prev @ [dst] in (* @ is needed to keep sources ordered *)
            IntMap.add src curr acc
          with Not_found ->
            IntMap.add src [dst] acc
        in
        loop new_acc other_src other_dst
    in
    loop IntMap.empty src_nodes dst_nodes
end

let dummy_node = Node.dummy ()

(* setup data server *)
let machine_file = ref ""
let chunk_size = ref Utils.default_chunk_size (* DAFT global constant *)
let local_state = ref FileSet.empty
let data_store_root = ref ""
let verbose = ref false
let delete_datastore = ref false

(* create local data store with unix UGO rights 700
   we take into account the port so that several DSs can be started on one
   host at the same time, for tests *)
let create_data_store local_node: string =
  assert(local_node <> dummy_node);
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root =
    sprintf "%s/%s.ds" tmp_dir (Node.to_string_id local_node)
  in
  Unix.mkdir data_store_root 0o700; (* access rights for owner only *)
  Log.info "I store in %s" data_store_root;
  data_store_root

let delete_data_store (ds: string): unit =
  let (_: int) = Sys.command ("rm -rf " ^ ds) in
  ()

(* how many there are and size of the last one if < chunk_size *)
let compute_chunks (size: int64) =
  let ratio = (Int64.to_float size) /. (float_of_int !chunk_size) in
  (* total number of chunks *)
  let nb_chunks = int_of_float (ceil ratio) in
  (* number of full chunks *)
  let nb_chunks_i64 = Int64.of_int (int_of_float ratio) in
  let chunk_size_i64 = Int64.of_int !chunk_size in
  let last_chunk_size_i64 = Int64.(size - (nb_chunks_i64 * chunk_size_i64)) in
  let last_chunk_size_opt =
    if last_chunk_size_i64 <> Int64.zero then Some last_chunk_size_i64
    else None
  in
  (nb_chunks, last_chunk_size_opt)

(* get the path of file fn in the local datastore *)
let fn_to_path (fn: Types.filename): string =
  !data_store_root ^
  if S.starts_with fn "/" then fn
  else "/" ^ fn

let add_file (local_node: Node.t) (src_fn: string) (dst_fn: string): ds_to_cli =
  if FileSet.contains_fn dst_fn !local_state then
    Fetch_file_cmd_nack (dst_fn, Already_here)
  else
    begin
      if not (Sys.file_exists src_fn) then
        Fetch_file_cmd_nack (src_fn, No_such_file)
      else
        begin
          if Sys.is_directory src_fn then
            Fetch_file_cmd_nack (src_fn, Is_directory)
          else FU.(
              let fn_stat = stat src_fn in
              let size = byte_of_size fn_stat.size in
              let dest_fn = fn_to_path dst_fn in
              let dest_dir = Fn.dirname dest_fn in
              (* mkdir creates all necessary parent dirs *)
              mkdir ~parent:true ~mode:0o700 dest_dir;
              cp ~follow:Follow ~force:Force ~recurse:false [src_fn] dest_fn;
              (* keep only read (and optionally exec) perms for the user *)
              if Utils.is_executable src_fn
              then Unix.chmod dest_fn 0o500
              else Unix.chmod dest_fn 0o400;
              (* check cp succeeded based on new file's size *)
              let stat' = stat dest_fn in
              if stat'.size <> fn_stat.size then
                Fetch_file_cmd_nack (src_fn, Copy_failed)
              else
                begin (* update local state *)
                  let nb_chunks, last_chunk_size = compute_chunks size in
                  let all_chunks =
                    File.all_chunks nb_chunks last_chunk_size local_node
                  in
                  let now = Unix.gettimeofday () in
                  let new_file = File.create dst_fn size now now nb_chunks all_chunks in
                  local_state := FileSet.add new_file !local_state;
                  Fetch_file_cmd_ack dst_fn
                end)
        end
    end

(* same return type than add_file, to keep the protocol small *)
let extract_file (src_fn: Types.filename) (dst_fn: Types.filename): ds_to_cli =
  if FileSet.contains_fn src_fn !local_state ||
     FileSet.contains_dir src_fn !local_state then
    if Sys.file_exists dst_fn then
      Fetch_file_cmd_nack (dst_fn, Already_here)
    else
      let dest_dir = Fn.dirname dst_fn in
      let in_data_store = fn_to_path src_fn in
      (* create all necessary parent dirs *)
      FU.mkdir ~parent:true ~mode:0o700 dest_dir;
      (* copy out of the datastore, even directories *)
      let _ = Unix.system (sprintf "cp -a %s %s" in_data_store dst_fn) in
      Fetch_file_cmd_ack dst_fn
  else
    Fetch_file_cmd_nack (src_fn, No_such_file)

(* retrieve a chunk from disk *)
let retrieve_chunk (fn: string) (chunk_id: int): string =
  try (* 1) check we have this file *)
    let file = FileSet.find_fn fn !local_state in
    try (* 2) check we have this chunk *)
      let chunk = File.find_chunk_id chunk_id file in
      (* 3) seek to it *)
      let local_file = fn_to_path fn in
      Utils.with_in_file_descr local_file (fun input ->
          let offset = chunk_id * !chunk_size in
          assert(Unix.(lseek input offset SEEK_SET) = offset);
          let curr_chunk_size = match Chunk.get_size chunk with
            | None -> !chunk_size
            | Some s ->
              let res = Int64.to_int s in
              assert(res > 0 && res < !chunk_size);
              res
          in
          (* WARNING: data copy here and allocation of
                      a fresh buffer each time *)
          let buff = String.create curr_chunk_size in
          Utils.really_read input buff curr_chunk_size;
          buff
        )
    with Not_found ->
      Utils.abort
        (Log.fatal "retrieve_chunk: no such chunk: fn: %s id: %d" fn chunk_id)
  with Not_found ->
    Utils.abort
      (Log.fatal "retrieve_chunk: no such file: %s" fn)

let send_chunk_to local_node int2node ds_rank something =
  (* enforce ds_rank bounds because array access soon *)
  match IntMap.find ds_rank int2node with
    | (_node, Some to_ds_i, _maybe_cli_sock) ->
      (* we only try to compress file data:
         the gain is too small for commands *)
      send rng msg_counter local_node to_ds_i something
    | (_, None, _maybe_cli_sock) ->
      Utils.abort
        (Log.fatal "send_chunk_to: no socket for DS %d" ds_rank)

let send_chunk local_node to_rank int2node fn chunk_id is_last chunk_data =
  let my_rank = Node.get_rank local_node in
  if to_rank = my_rank then
    Log.warn "send_chunk: to self: fn: %s cid: %d" fn chunk_id
  else
    let chunk_msg = DS_to_DS (Chunk (fn, chunk_id, is_last, chunk_data)) in
    send_chunk_to local_node int2node to_rank chunk_msg

let bcast_chunk_binary
    local_node to_ranks int2node fn chunk_id is_last root_rank step_num chunk_data =
  (* Log.debug "to_ranks: %s" (Utils.string_of_list string_of_int "; " to_ranks); *)
  let bcast_chunk_msg =
    DS_to_DS
      (Bcast_chunk_binary
         (fn, chunk_id, is_last, chunk_data, root_rank, step_num))
  in
  let my_rank = Node.get_rank local_node in
  List.iter (fun to_rank ->
      if to_rank <> my_rank then
        send_chunk_to local_node int2node to_rank bcast_chunk_msg
    ) to_ranks

let bcast_chunk_binomial
    local_node to_ranks int2node fn chunk_id is_last plan chunk_data =
  (* Log.debug "to_ranks: %s" (Utils.string_of_list string_of_int "; " to_ranks); *)
  let my_rank = Node.get_rank local_node in
  let remaining_plan = IntMap.remove my_rank plan in
  let bcast_chunk_msg =
    DS_to_DS
      (Bcast_chunk_binomial
         (fn, chunk_id, is_last, chunk_data, remaining_plan))
  in
  List.iter (fun to_rank ->
      send_chunk_to local_node int2node to_rank bcast_chunk_msg
    ) to_ranks

(* broadcast by only sending to next node (mod nb_nodes) in rank order *)
let relay_chunk_or_stop my_rank int2node root_rank raw_message =
  let nb_nodes = IntMap.cardinal int2node in
  let to_rank = (my_rank + 1) mod nb_nodes in
  if to_rank = root_rank then () (* stop *)
  else
    if to_rank >= 0 && to_rank < nb_nodes then
      match IntMap.find to_rank int2node with
      | (_node, Some to_ds_i, _maybe_cli_sock) -> send_as_is to_ds_i raw_message
      | (_, None, _maybe_cli_sock) ->
        Utils.abort
          (Log.fatal "relay_chunk_or_stop: no socket for DS %d" to_rank)
    else
      Log.warn "relay_chunk_or_stop: invalid to_rank: %d root_rank: %d"
        to_rank root_rank

let chain_broadcast local_node int2node fn chunk_id is_last root_rank =
  let nb_nodes = IntMap.cardinal int2node in
  let my_rank = Node.get_rank local_node in
  let to_rank = (my_rank + 1) mod nb_nodes in
  if to_rank = root_rank then () (* stop *)
  else
    let chunk_data = retrieve_chunk fn chunk_id in
    let relay_chunk_msg =
      DS_to_DS
        (Relay_chunk (fn, chunk_id, is_last, chunk_data, root_rank))
    in
    send_chunk_to local_node int2node to_rank relay_chunk_msg

let store_chunk local_node to_mds to_cli fn chunk_id is_last data =
  let file =
    try FileSet.find_fn fn !local_state (* retrieve it *)
    with Not_found -> (* or create it *)
      let unknown_size = Int64.of_int (-1) in
      let unknown_total_chunks = -1 in
      let now = Unix.gettimeofday () in
      let res =
        File.create fn unknown_size now now unknown_total_chunks ChunkSet.empty
      in
      local_state := FileSet.add res !local_state;
      res
  in
  if File.has_chunk file chunk_id then
    Log.warn "chunk already here: fn: %s cid: %d" fn chunk_id
  else
    let size = String.length data in
    let size64 = Int64.of_int size in
    let curr_chunk_size =
      if size <> !chunk_size then Some size64 else None
    in
    if not is_last && Option.is_some curr_chunk_size then
      Log.error "store_chunk: invalid chunk size: fn: %s id: %d size: %d"
        fn chunk_id size
    else begin
      (* 1) write chunk at offset *)
      let offset = chunk_id * !chunk_size in
      let local_file = fn_to_path fn in
      let dest_dir = Fn.dirname local_file in
      (* mkdir creates all necessary parent dirs *)
      FU.mkdir ~parent:true ~mode:0o700 dest_dir;
      Utils.with_out_file_descr local_file (fun out ->
          assert(Unix.(lseek out offset SEEK_SET) = offset);
          assert(Unix.write_substring out data 0 size = size)
        );
      (* 2) update local state *)
      let curr_chunk =
        File.Chunk.create chunk_id curr_chunk_size local_node
      in
      (* only once we receive the last chunk can we finalize the file's
         description in local_state *)
      let now = Unix.gettimeofday () in
      let new_file =
        if is_last then (* finalize file description *)
          let full_size = Int64.(of_int offset + size64) in
          let total_chunks = chunk_id + 1 in
          let chunks = File.get_chunks (File.add_chunk file curr_chunk now) in
          File.(create fn full_size file.creat_time now total_chunks chunks)
        else
          File.add_chunk file curr_chunk now
      in
      local_state := FileSet.update file new_file !local_state;
      (* 3) notify MDS *)
      let my_rank = Node.get_rank local_node in
      send rng msg_counter local_node to_mds
        (DS_to_MDS (Chunk_ack (fn, chunk_id, my_rank)));
      (* 4) notify CLI if all file chunks have been received
            and fix file perms in the local datastore *)
      let must_have = File.get_nb_chunks new_file in
      if must_have <> -1 then (* file has been finalized *)
        let curr_nb_chunks = ChunkSet.cardinal (File.get_chunks new_file) in
        if curr_nb_chunks = must_have then (* file is complete *)
          begin
            Unix.chmod local_file 0o400;
            match to_cli with
            | None -> () (* in case of broadcast: no feedback to CLI *)
            | Some cli_sock ->
              send rng msg_counter local_node cli_sock (DS_to_CLI (Fetch_file_cmd_ack fn))
          end
    end

let deref cli_sock_ref = match !cli_sock_ref with
  | None -> failwith "deref: to_cli socket uninitialized"
  | Some sock -> sock

let abort msg =
  Utils.abort (Log.fatal "%s" msg)

let main () =
  (* setup logger *)
  Log.set_log_level Log.INFO;
  Log.set_output Legacy.stderr;
  Log.color_on ();
  let log_fn = ref "" in
  (* options parsing *)
  let ds_port = ref Utils.default in
  Arg.parse
    [ "-cs", Arg.Set_int chunk_size, "<size> file chunk size";
      "-d", Arg.Set delete_datastore, " delete datastore at exit";
      "-o", Arg.Set_string log_fn, "<filename> where to log";
      "-m", Arg.Set_string machine_file,
      "machine_file list of host:port[:mds_port] (one per line)";
      "-p", Arg.Set_int ds_port, "<port> where the CLI is listening";
      "-v", Arg.Set verbose, " verbose mode"]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !verbose then Log.set_log_level Log.DEBUG;
  if !log_fn <> "" then Log.set_output (Legacy.open_out !log_fn);
  if !ds_port = Utils.default then abort "-p is mandatory";
  if !chunk_size = Utils.default then abort "-cs is mandatory";
  if !machine_file = "" then abort "-m is mandatory";
  let ctx = ZMQ.Context.create () in
  let hostname = Utils.hostname () in
  let skey, ckey, int2node, local_node', mds_node =
    Utils.data_nodes hostname (Some !ds_port) !machine_file
  in
  Socket_wrapper.setup_keys skey ckey;
  let local_node = ref (Option.get local_node') in
  let mds_host = Node.get_host mds_node in
  let mds_port = Node.get_port mds_node in
  let my_rank = Node.get_rank !local_node in
  Log.set_prefix (Utils.fg_yellow ^ (sprintf " DS-%d" my_rank) ^ Utils.fg_reset);
  assert(my_rank <> Utils.default);
  let ds_host = Node.get_host !local_node in
  assert(!ds_port = Node.get_port !local_node);
  (* create a push socket for each DS, except the current one because we
     will never send to self *)
  int2node :=
    IntMap.map (fun (node, ds_sock, cli_sock) ->
      if Node.get_rank node <> my_rank then
        let sock =
          Utils.(zmq_socket Push ctx (Node.get_host node) (Node.get_port node))
        in
        (node, Some sock, cli_sock)
      else
        (node, ds_sock, cli_sock)
    ) !int2node;
  let nb_nodes = Utils.IntMap.cardinal !int2node in
  Log.info "read %d host(s)" nb_nodes;
  Log.info "Client of MDS %s:%d" mds_host mds_port;
  data_store_root := create_data_store !local_node;
  (* setup server *)
  Log.info "binding server to %s:%d" "*" !ds_port;
  let incoming = Utils.(zmq_socket Pull ctx "*" !ds_port) in
  (* feedback socket to the local CLI *)
  let to_cli = ref None in
  (* register at the MDS *)
  Log.info "connecting to MDS %s:%d" mds_host mds_port;
  let to_mds = Utils.(zmq_socket Push ctx mds_host mds_port) in
  send rng msg_counter !local_node to_mds (DS_to_MDS (Join_push !local_node));
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      Log.debug "waiting msg";
      let message' = receive incoming in
      match message' with
      | None -> Log.warn "junk"
      | Some (message, raw_message) ->
        match message with
        | MDS_to_DS (Send_to_req (to_rank, fn, chunk_id, is_last)) ->
          begin
            Log.debug "got Send_to_req";
            let chunk_data = retrieve_chunk fn chunk_id in
            send_chunk !local_node to_rank !int2node fn chunk_id is_last chunk_data
          end
        | MDS_to_DS Quit_cmd ->
          Log.info "got Quit_cmd";
          Utils.nuke_file !machine_file;
          not_finished := false
        | MDS_to_DS (Add_file_ack fn) ->
          Log.debug "got Add_file_ack";
          (* forward the good news to the CLI *)
          send rng msg_counter !local_node (deref to_cli)
            (DS_to_CLI (Fetch_file_cmd_ack fn))
        | MDS_to_DS (Add_file_nack fn) ->
          Log.debug "got Add_file_nack";
          Log.warn "datastore: no rollback for %s" fn;
          (* quick and dirty but maybe OK: only rollback local state's view *)
          local_state := FileSet.remove_fn fn !local_state;
          (* forward nack to CLI *)
          send rng msg_counter !local_node (deref to_cli)
            (DS_to_CLI (Fetch_file_cmd_nack (fn, Already_here)))
        | DS_to_DS (Chunk (fn, chunk_id, is_last, data)) ->
          Log.debug "got Chunk";
          store_chunk !local_node to_mds !to_cli fn chunk_id is_last data
        | CLI_to_DS (Fetch_file_cmd_req (src_fn, dst_fn, Local)) ->
          Log.debug "got Fetch_file_cmd_req:Local";
          let res = add_file !local_node src_fn dst_fn in
          begin match res with
            | Fetch_file_cmd_ack fn ->
              (* notify MDS about this new file *)
              let file = FileSet.find_fn fn !local_state in
              let add_file_req = Add_file_req (my_rank, file) in
              send rng msg_counter !local_node to_mds (DS_to_MDS add_file_req)
            | Fetch_file_cmd_nack (fn, err) ->
              send rng msg_counter !local_node (deref to_cli)
                (DS_to_CLI (Fetch_file_cmd_nack (fn, err)))
            | Bcast_file_ack -> assert(false)
          end
        | CLI_to_DS (Bcast_file_cmd_req (fn, bcast_method)) ->
          Log.debug "got Bcast_file_cmd_req";
          let res = add_file !local_node fn fn in
          begin match res with
            | Fetch_file_cmd_nack (fn, Already_here) (* allow bcast after put *)
            | Fetch_file_cmd_ack fn ->
	      (* here starts the true business of the broadcast *)
              let file = FileSet.find_fn fn !local_state in
              let last_cid = (File.get_nb_chunks file) - 1 in
              let bcast_file_req = Bcast_file_req (my_rank, file, bcast_method) in
              (* notify MDS to allow this potentially new file *)
	      send rng msg_counter !local_node to_mds (DS_to_MDS bcast_file_req);
              (* unlock CLI !!! TURNED OFF FOR TIMING EXPERIMENTS !!! *)
              (* send rng msg_counter !local_node (deref to_cli) *)
              (*   (DS_to_CLI Bcast_file_ack); *)
              begin match bcast_method with
                | Binary ->
                  begin
                    match Binary.fork my_rank 0 my_rank nb_nodes with
                    | [], _ -> () (* job done *)
                    | (to_ranks, step_num) ->
                      match to_ranks with
                      | [_] | [_; _] -> (* one or two successors *)
                        for chunk_id = 0 to last_cid do
                          let chunk_data = retrieve_chunk fn chunk_id in
                          let is_last = (chunk_id = last_cid) in
                          bcast_chunk_binary
                            !local_node to_ranks !int2node fn chunk_id is_last
                            my_rank step_num chunk_data
                        done
                      | _ -> assert(false)
                  end
                | Chain ->
                  for chunk_id = 0 to last_cid do
                    let is_last = (chunk_id = last_cid) in
                    chain_broadcast
                      !local_node !int2node fn chunk_id is_last my_rank
                  done
                | Binomial ->
                  let plan = Binomial.plan my_rank nb_nodes in
                  let to_ranks = IntMap.find my_rank plan in
                  for chunk_id = 0 to last_cid do
                    let chunk_data = retrieve_chunk fn chunk_id in
                    let is_last = (chunk_id = last_cid) in
                    bcast_chunk_binomial
                      !local_node to_ranks !int2node fn chunk_id is_last plan chunk_data
                  done
              end
            | Fetch_file_cmd_nack (fn, err) ->
              send rng msg_counter !local_node (deref to_cli)
                (DS_to_CLI (Fetch_file_cmd_nack (fn, err)))
            | Bcast_file_ack -> assert(false)
	  end
	| DS_to_DS
            (Bcast_chunk_binary
               (fn, chunk_id, is_last, chunk_data, root_rank, step_num)) ->
          begin
            Log.debug "got Bcast_chunk_binary";
            store_chunk !local_node to_mds None fn chunk_id is_last chunk_data;
            match Binary.fork root_rank step_num my_rank nb_nodes with
            | ([], _) -> () (* job done *)
            | (to_ranks, step_num) ->
              match to_ranks with
              | [_] | [_; _] -> (* one or two successors *)
                bcast_chunk_binary
                  !local_node to_ranks !int2node fn chunk_id is_last
                  root_rank step_num chunk_data
              | _ -> assert(false)
          end
	| DS_to_DS
            (Bcast_chunk_binomial
               (fn, chunk_id, is_last, chunk_data, plan)) ->
          begin
            Log.debug "got Bcast_chunk_binomial";
            store_chunk !local_node to_mds None fn chunk_id is_last chunk_data;
            try
              (* I have to continue this broadcast *)
              let to_ranks = IntMap.find my_rank plan in
              bcast_chunk_binomial
                !local_node to_ranks !int2node fn chunk_id is_last plan chunk_data
            with Not_found -> ()
          end
        | CLI_to_DS (Fetch_file_cmd_req (src_fn, dst_fn, Remote)) ->
          Log.debug "got Fetch_file_cmd_req:Remote";
          (* finish quickly in case file is already present locally *)
          if FileSet.contains_fn  dst_fn !local_state ||
             FileSet.contains_dir dst_fn !local_state then
            let _ = Log.info "%s already here" dst_fn in
            send rng msg_counter !local_node (deref to_cli)
              (DS_to_CLI (Fetch_file_cmd_ack src_fn))
          else (* forward request to MDS *)
            (* FBR: maybe there is a bug: fetch of a remote file never terminate in the CLI ? *)
            send rng msg_counter !local_node to_mds
              (DS_to_MDS (Fetch_file_req (Node.get_rank !local_node, dst_fn)))
        | CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn)) ->
          let res = extract_file src_fn dst_fn in
          send rng msg_counter !local_node (deref to_cli) (DS_to_CLI res)
        | CLI_to_DS (Connect_cmd_push cli_port) ->
          Log.debug "got Connect_cmd_push";
          (* setup socket to CLI *)
          to_cli := Some (Utils.(zmq_socket Push ctx ds_host cli_port));
          (* complete local_node *)
          assert(Node.get_cli_port !local_node = None);
          local_node := Node.create my_rank ds_host !ds_port (Some cli_port)
	| DS_to_DS
            (Relay_chunk (fn, chunk_id, is_last, chunk_data, root_rank)) ->
          begin
            Log.debug "got Relay_chunk";
            store_chunk !local_node to_mds None fn chunk_id is_last chunk_data;
            relay_chunk_or_stop my_rank !int2node root_rank raw_message
          end
    done;
    raise Types.Loop_end;
  with exn -> begin
      Socket_wrapper.nuke_keys ();
      Utils.nuke_CSPRNG rng;
      if !delete_datastore then delete_data_store !data_store_root;
      ZMQ.Socket.close incoming;
      ZMQ.Socket.close to_mds;
      (match !to_cli with
       | Some s -> ZMQ.Socket.close s;
       | None -> () (* no CLI ever registered with us *)
      );
      let dont_warn = false in
      Utils.cleanup_data_nodes dont_warn !int2node;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end

let () = main ()

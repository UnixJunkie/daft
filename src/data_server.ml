open Batteries
open Legacy.Printf
open Types.Protocol

let ds_in_yellow = Utils.fg_yellow ^ "DS" ^ Utils.fg_reset

module A  = Array
module Fn = Filename
module FU = FileUtil
module IntMap = Map.Make(Int)
module IntSet = Set.Make(Int)
module Logger = Log
(* prefix all logs *)
module Log = Log.Make (struct let section = ds_in_yellow end)
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet
module Chunk = File.Chunk
module ChunkSet = File.ChunkSet

let send, receive = Socket_wrapper.DS_socket.(send, receive)

let msg_counter = ref 0

module Amoeba = struct
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
module Well_exhaust = struct
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

(* setup data server *)
let ds_log_fn = ref ""
let ds_host = ref "localhost"
let ds_port_in = ref Utils.default_ds_port_in
let my_rank = ref Utils.uninitialized
let machine_file = ref ""
let mds_host = ref "localhost"
let mds_port_in = ref Utils.default_mds_port_in
let chunk_size = ref Utils.default_chunk_size (* DAFT global constant *)
let local_state = ref FileSet.empty
let data_store_root = ref ""
let local_node = ref (Node.dummy ()) (* this node *)
let verbose = ref false
let delete_datastore = ref false

let abort msg =
  Log.fatal "%s" msg;
  exit 1

(* create local data store with unix UGO rights 700
   we take into account the port so that several DSs can be started on one
   host at the same time, for tests *)
let create_data_store (): string =
  assert(!local_node <> Node.dummy ());
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root =
    sprintf "%s/%s.ds" tmp_dir (Node.to_string_id !local_node)
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

let add_file (fn: string): ds_to_cli =
  if FileSet.contains_fn fn !local_state then
    Fetch_file_cmd_nack (fn, Already_here)
  else begin
    if not (Sys.file_exists fn) then
      Fetch_file_cmd_nack (fn, No_such_file)
    else begin
      if Sys.is_directory fn then
        Fetch_file_cmd_nack (fn, Is_directory)
      else FU.(
          let stat = FU.stat fn in
          let size = FU.byte_of_size stat.size in
          let dest_fn = fn_to_path fn in
          let dest_dir = Fn.dirname dest_fn in
          (* mkdir creates all necessary parent dirs *)
          FU.mkdir ~parent:true ~mode:0o700 dest_dir;
          FU.cp ~follow:FU.Follow ~force:FU.Force ~recurse:false [fn] dest_fn;
          (* keep only read (and optionally exec) perms for the user *)
          if Utils.is_executable fn
          then Unix.chmod dest_fn 0o500
          else Unix.chmod dest_fn 0o400;
          (* check cp succeeded based on new file's size *)
          let stat' = FU.stat dest_fn in
          if stat'.size <> stat.size then
            Fetch_file_cmd_nack (fn, Copy_failed)
          else begin (* update local state *)
            let nb_chunks, last_chunk_size = compute_chunks size in
            let all_chunks =
              File.all_chunks nb_chunks last_chunk_size !local_node
            in
            let new_file = File.create fn size nb_chunks all_chunks in
            local_state := FileSet.add new_file !local_state;
            Fetch_file_cmd_ack fn
          end)
    end
  end

(* same return type than add_file, to keep the protocol small *)
let extract_file (src_fn: Types.filename) (dst_fn: Types.filename): ds_to_cli =
  if FileSet.contains_fn src_fn !local_state then
    if Utils.file_or_link_exists dst_fn then
      Fetch_file_cmd_nack (dst_fn, Already_here)
    else
      let dest_dir = Fn.dirname dst_fn in
      let in_data_store = fn_to_path src_fn in
      (* create all necessary parent dirs *)
      FU.mkdir ~parent:true ~mode:0o700 dest_dir;
      (* only soft link for the moment since it's fast *)
      Unix.symlink in_data_store dst_fn;
      Fetch_file_cmd_ack dst_fn
  else
    Fetch_file_cmd_nack (src_fn, No_such_file)

exception Fatal (* throw this when we are doomed *)

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
      begin
        Log.fatal "retrieve_chunk: no such chunk: fn: %s id: %d" fn chunk_id;
        raise Fatal
      end
  with Not_found ->
    begin
      Log.fatal "retrieve_chunk: no such file: %s" fn;
      raise Fatal
    end

let send_chunk_to local_node int2node ds_rank something =
  (* enforce ds_rank bounds because array access soon *)
  if Utils.out_of_bounds ds_rank int2node then
    begin
      Log.fatal "send_chunk_to: out of bounds rank: %d" ds_rank;
      raise Fatal
    end
  else match int2node.(ds_rank) with
    | (_node, Some to_ds_i, _maybe_cli_sock) ->
      (* we only try to compress file data:
         the gain is too small for commands *)
      send ~compress:true msg_counter local_node to_ds_i something
    | (_, None, _maybe_cli_sock) ->
      begin
        Log.fatal "send_chunk_to: no socket for DS %d" ds_rank;
        raise Fatal
      end

let send_chunk local_node to_rank int2node fn chunk_id is_last chunk_data =
  if to_rank = !my_rank then
    Log.warn "send_chunk: to self: fn: %s cid: %d" fn chunk_id
  else
    let chunk_msg = DS_to_DS (Chunk (fn, chunk_id, is_last, chunk_data)) in
    send_chunk_to local_node int2node to_rank chunk_msg

let bcast_chunk_amoeba
    local_node to_ranks int2node fn chunk_id is_last root_rank step_num chunk_data =
  (* Log.debug "to_ranks: %s" (Utils.string_of_list string_of_int "; " to_ranks); *)
  let bcast_chunk_msg =
    DS_to_DS
      (Bcast_chunk_amoeba (fn, chunk_id, is_last, chunk_data, root_rank, step_num))
  in
  List.iter (fun to_rank ->
      if to_rank <> !my_rank then
        send_chunk_to local_node int2node to_rank bcast_chunk_msg
    ) to_ranks

let bcast_chunk_well_exhaust
    local_node to_ranks int2node fn chunk_id is_last plan chunk_data =
  (* Log.debug "to_ranks: %s" (Utils.string_of_list string_of_int "; " to_ranks); *)
  let remaining_plan = IntMap.remove !my_rank plan in
  let bcast_chunk_msg =
    DS_to_DS
      (Bcast_chunk_well_exhaust
         (fn, chunk_id, is_last, chunk_data, remaining_plan))
  in
  List.iter (fun to_rank ->
      send_chunk_to local_node int2node to_rank bcast_chunk_msg
    ) to_ranks

(* broadcast by only sending to next node (next_node_rank = my_rank + 1) *)
let relay_chunk_or_stop local_node int2node fn chunk_id is_last root_rank chunk_data =
  let nb_nodes = A.length int2node in
  let to_rank = (!my_rank + 1) mod nb_nodes in
  if to_rank <> root_rank then
    let relay_chunk_msg =
      DS_to_DS
        (Relay_chunk (fn, chunk_id, is_last, chunk_data, root_rank))
    in
    send_chunk_to local_node int2node to_rank relay_chunk_msg

let store_chunk to_mds to_cli fn chunk_id is_last data =
  let file =
    try FileSet.find_fn fn !local_state (* retrieve it *)
    with Not_found -> (* or create it *)
      let unknown_size = Int64.of_int (-1) in
      let unknown_total_chunks = -1 in
      File.create
        fn unknown_size unknown_total_chunks ChunkSet.empty
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
        File.Chunk.create chunk_id curr_chunk_size !local_node
      in
      (* only once we receive the last chunk can we finalize the file's
         description in local_state *)
      let new_file =
        if is_last then (* finalize file description *)
          let full_size = Int64.(of_int offset + size64) in
          let total_chunks = chunk_id + 1 in
          let chunks = File.get_chunks (File.add_chunk file curr_chunk) in
          File.create fn full_size total_chunks chunks
        else
          File.add_chunk file curr_chunk
      in
      local_state := FileSet.update new_file !local_state;
      (* 3) notify MDS *)
      send msg_counter !local_node to_mds
        (DS_to_MDS (Chunk_ack (fn, chunk_id, !my_rank)));
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
              send msg_counter !local_node cli_sock (DS_to_CLI (Fetch_file_cmd_ack fn))
          end
    end

let deref cli_sock_ref = match !cli_sock_ref with
  | None -> failwith "deref: to_cli socket uninitialized"
  | Some sock -> sock

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.INFO;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  ds_host := Utils.hostname ();
  (* options parsing *)
  Arg.parse
    [ "-cs", Arg.Set_int chunk_size, "<size> file chunk size";
      "-d", Arg.Set delete_datastore, " delete datastore at exit";
      "-l", Arg.Set_string ds_log_fn, "<filename> where to log";
      "-p", Arg.Set_int ds_port_in, "<port> where to listen";
      "-r", Arg.Set_int my_rank, "<rank> rank among other data nodes";
      "-m", Arg.Set_string machine_file,
      "machine_file list of [user@]host:port (one per line)";
      "-mds", Arg.Set_string mds_host, "<server> MDS host";
      "-mdsp", Arg.Set_int mds_port_in, "<port> MDS port";
      "-v", Arg.Set verbose, " verbose mode"]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !verbose then Logger.set_log_level Logger.DEBUG;
  if !ds_log_fn <> "" then begin (* don't log anything before that *)
    let log_out = Legacy.open_out !ds_log_fn in
    Logger.set_output log_out
  end;
  if !chunk_size = Utils.uninitialized then abort "-cs is mandatory";
  if !mds_host = "" then abort "-mds is mandatory";
  if !mds_port_in = Utils.uninitialized then abort "-sp is mandatory";
  if !my_rank = Utils.uninitialized then abort "-r is mandatory";
  if !ds_port_in = Utils.uninitialized then abort "-p is mandatory";
  if !machine_file = "" then abort "-m is mandatory";
  let ctx = ZMQ.Context.create () in
  let int2node = Utils.data_nodes_array !machine_file in
  (* create a push socket for each DS, except the current one because we
     will never send to self *)
  A.iteri (fun i (node, _ds_sock, cli_sock) ->
      if i <> !my_rank then
        let sock =
          Utils.(zmq_socket Push ctx (Node.get_host node) (Node.get_ds_port node))
        in
        A.set int2node i (node, Some sock, cli_sock)
    ) int2node;
  let nb_nodes = A.length int2node in
  Log.info "read %d host(s)" nb_nodes;
  local_node := Node.create !my_rank !ds_host !ds_port_in None;
  Log.info "Client of MDS %s:%d" !mds_host !mds_port_in;
  data_store_root := create_data_store ();
  (* setup server *)
  Log.info "binding server to %s:%d" "*" !ds_port_in;
  let incoming = Utils.(zmq_socket Pull ctx "*" !ds_port_in) in
  (* feedback socket to the local CLI *)
  let to_cli = ref None in
  (* register at the MDS *)
  Log.info "connecting to MDS %s:%d" !mds_host !mds_port_in;
  let to_mds = Utils.(zmq_socket Push ctx !mds_host !mds_port_in) in
  send msg_counter !local_node to_mds (DS_to_MDS (Join_push !local_node));
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      Log.debug "waiting msg";
      let message' = receive incoming in
      match message' with
      | None -> Log.warn "junk"
      | Some message ->
        match message with
        | MDS_to_DS (Send_to_req (to_rank, fn, chunk_id, is_last)) ->
          begin
            Log.debug "got Send_to_req";
            let chunk_data = retrieve_chunk fn chunk_id in
            send_chunk !local_node to_rank int2node fn chunk_id is_last chunk_data
          end
        | MDS_to_DS Quit_cmd ->
          Log.info "got Quit_cmd";
          not_finished := false
        | MDS_to_DS (Add_file_ack fn) ->
          Log.debug "got Add_file_ack";
          (* forward the good news to the CLI *)
          send msg_counter !local_node (deref to_cli)
            (DS_to_CLI (Fetch_file_cmd_ack fn))
        | MDS_to_DS (Add_file_nack fn) ->
          Log.debug "got Add_file_nack";
          Log.warn "datastore: no rollback for %s" fn;
          (* quick and dirty but maybe OK: only rollback local state's view *)
          local_state := FileSet.remove_fn fn !local_state;
          (* forward nack to CLI *)
          send msg_counter !local_node (deref to_cli)
            (DS_to_CLI (Fetch_file_cmd_nack (fn, Already_here)))
        | DS_to_DS (Chunk (fn, chunk_id, is_last, data)) ->
          Log.debug "got Chunk";
          store_chunk to_mds !to_cli fn chunk_id is_last data
        | CLI_to_DS (Fetch_file_cmd_req (fn, Local)) ->
          Log.debug "got Fetch_file_cmd_req:Local";
          let res = add_file fn in
          begin match res with
            | Fetch_file_cmd_ack fn ->
              (* notify MDS about this new file *)
              let file = FileSet.find_fn fn !local_state in
              let add_file_req = Add_file_req (!my_rank, file) in
              send msg_counter !local_node to_mds (DS_to_MDS add_file_req)
            | Fetch_file_cmd_nack (fn, err) ->
              send msg_counter !local_node (deref to_cli)
                (DS_to_CLI (Fetch_file_cmd_nack (fn, err)))
            | Bcast_file_ack -> assert(false)
          end
        | CLI_to_DS (Bcast_file_cmd_req (fn, bcast_method)) ->
          Log.debug "got Bcast_file_cmd_req";
          let res = add_file fn in
          begin match res with
            | Fetch_file_cmd_nack (fn, Already_here) (* allow bcast after put *)
            | Fetch_file_cmd_ack fn ->
	      (* here starts the true business of the broadcast *)
              let file = FileSet.find_fn fn !local_state in
              let last_cid = (File.get_nb_chunks file) - 1 in
              let bcast_file_req = Bcast_file_req file in
              (* notify MDS to allow this potentially new file *)
	      send msg_counter !local_node to_mds (DS_to_MDS bcast_file_req);
              (* unlock CLI *)
              send msg_counter !local_node (deref to_cli)
                (DS_to_CLI Bcast_file_ack);
              begin match bcast_method with
                | Amoeba ->
                  begin
                    match Amoeba.fork !my_rank 0 !my_rank nb_nodes with
                    | [], _ -> () (* job done *)
                    | (to_ranks, step_num) ->
                      match to_ranks with
                      | [_] | [_; _] -> (* one or two successors *)
                        for chunk_id = 0 to last_cid do
                          let chunk_data = retrieve_chunk fn chunk_id in
                          let is_last = (chunk_id = last_cid) in
                          bcast_chunk_amoeba
                            !local_node to_ranks int2node fn chunk_id is_last
                            !my_rank step_num chunk_data
                        done
                      | _ -> assert(false)
                  end
                | Relay ->
                  for chunk_id = 0 to last_cid do
                    let chunk_data = retrieve_chunk fn chunk_id in
                    let is_last = (chunk_id = last_cid) in
                    relay_chunk_or_stop
                      !local_node int2node fn chunk_id is_last !my_rank chunk_data
                  done
                | Well_exhaust ->
                  let plan = Well_exhaust.plan !my_rank nb_nodes in
                  let to_ranks = IntMap.find !my_rank plan in
                  for chunk_id = 0 to last_cid do
                    let chunk_data = retrieve_chunk fn chunk_id in
                    let is_last = (chunk_id = last_cid) in
                    bcast_chunk_well_exhaust
                      !local_node to_ranks int2node fn chunk_id is_last plan chunk_data
                  done                  
              end
            | Fetch_file_cmd_nack (fn, err) ->
              send msg_counter !local_node (deref to_cli)
                (DS_to_CLI (Fetch_file_cmd_nack (fn, err)))
            | Bcast_file_ack -> assert(false)
	  end
	| DS_to_DS
            (Bcast_chunk_amoeba
               (fn, chunk_id, is_last, chunk_data, root_rank, step_num)) ->
          begin
            Log.debug "got Bcast_chunk_amoeba";
            store_chunk to_mds None fn chunk_id is_last chunk_data;
            match Amoeba.fork root_rank step_num !my_rank nb_nodes with
            | ([], _) -> () (* job done *)
            | (to_ranks, step_num) ->
              match to_ranks with
              | [_] | [_; _] -> (* one or two successors *)
                bcast_chunk_amoeba !local_node to_ranks int2node fn chunk_id is_last
                  root_rank step_num chunk_data
              | _ -> assert(false)
          end
	| DS_to_DS
            (Bcast_chunk_well_exhaust
               (fn, chunk_id, is_last, chunk_data, plan)) ->
          begin
            Log.debug "got Bcast_chunk_well_exhaust";
            store_chunk to_mds None fn chunk_id is_last chunk_data;
            try
              (* I have to continue this broadcast *)
              let to_ranks = IntMap.find !my_rank plan in
              bcast_chunk_well_exhaust
                !local_node to_ranks int2node fn chunk_id is_last plan chunk_data
            with Not_found -> ()
          end
        | CLI_to_DS (Fetch_file_cmd_req (fn, Remote)) ->
          Log.debug "got Fetch_file_cmd_req:Remote";
          (* finish quickly in case file is already present locally *)
          if FileSet.contains_fn fn !local_state then
            let _ = Log.info "%s already here" fn in
            send msg_counter !local_node (deref to_cli)
              (DS_to_CLI (Fetch_file_cmd_ack fn))
          else (* forward request to MDS *)
            send msg_counter !local_node to_mds
              (DS_to_MDS (Fetch_file_req (Node.get_rank !local_node, fn)))
        | CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn)) ->
          let res = extract_file src_fn dst_fn in
          send msg_counter !local_node (deref to_cli) (DS_to_CLI res)
        | CLI_to_DS (Connect_cmd_push cli_port) ->
          Log.debug "got Connect_cmd_push";
          (* setup socket to CLI *)
          to_cli := Some (Utils.(zmq_socket Push ctx !ds_host cli_port));
          (* complete local_node *)
          assert(Node.get_cli_port !local_node = None);
          local_node := Node.create !my_rank !ds_host !ds_port_in (Some cli_port)
	| DS_to_DS
            (Relay_chunk (fn, chunk_id, is_last, chunk_data, root_rank)) ->
          begin
            Log.debug "got Relay_chunk";
            store_chunk to_mds None fn chunk_id is_last chunk_data;
            relay_chunk_or_stop
              !local_node int2node fn chunk_id is_last root_rank chunk_data
          end
    done;
    raise Types.Loop_end;
  with exn -> begin
      if !delete_datastore then delete_data_store !data_store_root;
      ZMQ.Socket.close incoming;
      ZMQ.Socket.close to_mds;
      (match !to_cli with
       | Some s -> ZMQ.Socket.close s;
       | None -> () (* no CLI ever registered with us *)
      );
      let dont_warn = false in
      Utils.cleanup_data_nodes_array dont_warn int2node;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end

let () = main ()

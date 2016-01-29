open Batteries
open Legacy.Printf
open Types.Protocol

module Ht = Hashtbl
module FileSet = Types.FileSet
module File = Types.File
module Chunk = Types.File.Chunk
module IntMap = Types.IntMap
module L = List
module Node = Types.Node

let global_state = ref FileSet.empty
let msg_counter = ref 0

let bcast_start = ref (Unix.gettimeofday ())
let bcast_end = ref !bcast_start
let bcast_root_rank = ref (-1)
let bcast_algo = ref Binomial
let rng = Utils.create_CSPRNG ()

let send, receive = Socket_wrapper.MDS_socket.(send, receive)

let exec_ls_command (detailed: bool) (maybe_fn: string option): FileSet.t =
  match maybe_fn with
  | None ->
    if detailed then !global_state
    else FileSet.forget_chunks !global_state
  | Some fn ->
    try
      let f = FileSet.find_fn fn !global_state in
      FileSet.singleton
        (if detailed then f
         else File.forget_chunks f)
    with Not_found -> FileSet.empty

let start_data_nodes _machine_fn =
  (* TODO: scp exe to each node *)
  (* TODO: ssh node to start it *)
  ()

let fetch_mds local_node fn ds_rank int2node feedback_to_cli =
  try
    (* FBR: make this work for a directory *)
    let file = FileSet.find_fn fn !global_state in
    let chunks = Types.File.get_chunks file in
    (* FBR: code logic needs to move into the File module
            File.get_rand_sources will send a list of
            (chunk_rank, src_node) *)
    (* randomized algorithm: for each chunk we ask a randomly selected
       chunk source to send the chunk to destination *)
    Types.File.ChunkSet.iter (fun chunk ->
        let chunk_id = Chunk.get_id chunk in
        (* cache lookup to not send if already there *)
        if Chunk.has_source_nid chunk ds_rank then
          Log.warn "fetch_mds: chunk already on node: fn: %s cid: %d nid: %d"
            fn chunk_id ds_rank
        else
          let selected_src_node = Chunk.select_source_rand chunk in
          let chosen = Node.get_rank selected_src_node in
          match IntMap.find chosen int2node with
          | (_node, Some to_ds_i, _maybe_cli_sock) ->
            let is_last = File.is_last_chunk chunk file in
            let send_order =
              MDS_to_DS
                (Send_to_req (ds_rank, fn, chunk_id, is_last))
            in
            send rng msg_counter local_node to_ds_i send_order
          | (_, None, _) -> assert(false)
      ) chunks
  with Not_found ->
    if feedback_to_cli then
      match Utils.trd3 (IntMap.find ds_rank int2node) with
      | None -> Log.warn "fetch_mds: no CLI feedback sock for node %d"
                  ds_rank
      | Some to_cli_sock -> (* unlock CLI *)
        send rng msg_counter local_node to_cli_sock
          (MDS_to_CLI (Fetch_cmd_nack fn))

let main () =
  (* setup logger *)
  Log.set_log_level Log.INFO;
  Log.set_output Legacy.stderr;
  Log.color_on ();
  Log.set_prefix (Colors.fg_yellow ^ " MDS" ^ Colors.fg_reset);
  (* setup MDS *)
  let machine_file = ref "" in
  let verbose = ref false in
  let log_fn = ref "" in
  Arg.parse
    [ "-m", Arg.Set_string machine_file,
      "machine_file list of [user@]host:port (one per line)";
      "-o", Arg.Set_string log_fn, "<filename> where to log";
      "-v", Arg.Set verbose, " verbose mode"]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !verbose then Log.set_log_level Log.DEBUG;
  if !log_fn <> "" then Log.set_output (Legacy.open_out !log_fn);
  if !machine_file = "" then Utils.abort (Log.fatal "-m is mandatory");
  let hostname = Utils.hostname () in
  Utils.append_keys !machine_file;
  let skey, ckey, int2node, _local_ds_node, local_node =
    Utils.data_nodes hostname None !machine_file
  in
  Socket_wrapper.setup_keys skey ckey;
  Log.info "read %d host(s)" (IntMap.cardinal !int2node);
  start_data_nodes !machine_file;
  let mds_port = Node.get_port local_node in
  (* start server *)
  Log.info "binding server to %s:%d" "*" mds_port;
  let ctx = ZMQ.Context.create () in
  let incoming = Utils.(zmq_socket Pull ctx "*" mds_port) in
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      Log.debug "waiting msg";
      let message' = receive incoming in
      Log.debug "got msg";
      match message' with
      | None -> Log.warn "junk"
      | Some message ->
        match message with
        | DS_to_MDS (Join_push ds) ->
          Log.debug "got Join_push";
          let ds_as_string = Node.to_string ds in
          Log.info "DS %s Join req" ds_as_string;
          let ds_rank, ds_host, ds_port_in, _ds_cli_port = Node.to_quad ds in
          (* check it is the one we expect at that rank *)
          let expected_ds, prev_ds_sock, prev_cli_sock =
            IntMap.find ds_rank !int2node
          in
          begin match prev_ds_sock with
            | Some _ -> Log.warn "%s already joined" ds_as_string
            | None -> (* remember him for the future *)
              if ds = expected_ds then
                let sock = Utils.(zmq_socket Push ctx ds_host ds_port_in) in
                int2node :=
                  IntMap.add ds_rank (ds, Some sock, prev_cli_sock) !int2node
              else
                Log.warn "suspicious Join req from %s" ds_as_string;
          end
        | DS_to_MDS (Add_file_req (ds_rank, f)) ->
          Log.debug "got Add_file_req";
          let open Types.File in
          begin match Utils.snd3 (IntMap.find ds_rank !int2node) with
            | None -> Log.warn "cannot send nack of %s to %d" f.name ds_rank
            | Some receiver ->
              let ack_or_nack =
                if FileSet.contains_fn f.name !global_state then
                  (* already one with same name *)
                  Add_file_nack f.name
                else begin
                  global_state := FileSet.add f !global_state;
                  Add_file_ack f.name
                end
              in
              send rng msg_counter local_node receiver (MDS_to_DS ack_or_nack)
          end
        | DS_to_MDS (Chunk_ack (fn, chunk_id, ds_rank)) ->
          begin
            let now = Unix.gettimeofday () in
            Log.debug "got Chunk_ack";
            try
              (* 1) do we have this file ? *)
              let old_file = FileSet.find_fn fn !global_state in
              try
                (* 2) does it have this chunk ? *)
                let prev_chunk = File.find_chunk_id chunk_id old_file in
                (* 3) update global state: this chunk is owned by one more DS *)
                let new_source = Utils.fst3 (IntMap.find ds_rank !int2node) in
                let new_chunk = Chunk.add_source prev_chunk new_source in
                let new_file = File.update_chunk old_file new_chunk now in
                global_state := FileSet.update old_file new_file !global_state
              with Not_found ->
                begin
                  Log.warn "Chunk_ack: unknown (scat?) chunk: %s chunk_id: %d"
                    fn chunk_id;
                  let new_chunk =
                    File.create_scat_chunk old_file chunk_id Utils.default_chunk_size
                  in
                  (* update global state: this chunk is owned by one DS *)
                  let new_source = Utils.fst3 (IntMap.find ds_rank !int2node) in
                  let new_chunk = Chunk.add_source new_chunk new_source in
                  let new_file = File.add_chunk old_file new_chunk now in
                  global_state := FileSet.update old_file new_file !global_state
                end
            with Not_found ->
              Log.error "Chunk_ack: unknown file: %s chunk_id: %d"
                fn chunk_id
          end
        | DS_to_MDS (Fetch_file_req (ds_rank, fn)) ->
	  Log.debug "got Fetch_file_req";
	  fetch_mds local_node fn ds_rank !int2node true
        | DS_to_MDS (Bcast_file_req (ds_rank, f, bcast_method)) ->
          begin
            bcast_start := Unix.gettimeofday();
            bcast_root_rank := ds_rank;
            bcast_algo := bcast_method;
            Log.debug "got Bcast_file_req";
            if FileSet.mem f !global_state then
              Log.warn "file already added: %s" (File.(f.name))
            else
              global_state := FileSet.add f !global_state;
          end
        | DS_to_MDS (Scat_file_req f) ->
          let () = Log.debug "got Scat_file_req" in
          if FileSet.mem f !global_state then
            Log.error "file already here: %s" (File.(f.name))
          else
            global_state := FileSet.add f !global_state;
        | CLI_to_MDS (Connect_push (ds_rank, cli_port)) ->
          Log.debug "got Connect_push rank: %d port: %d" ds_rank cli_port;
          let node, ds_sock, maybe_cli_sock = IntMap.find ds_rank !int2node in
          assert(Option.is_some ds_sock); (* already a DS sock *)
          assert(Option.is_none maybe_cli_sock); (* not yet a CLI sock *)
          let cli_sock = Utils.(zmq_socket Push ctx (Node.get_host node) cli_port) in
          int2node := IntMap.add ds_rank (node, ds_sock, Some cli_sock) !int2node
        | CLI_to_MDS (Ls_cmd_req (ds_rank, detailed, maybe_fn)) ->
          Log.debug "got Ls_cmd_req";
          begin match Utils.trd3 (IntMap.find ds_rank !int2node) with
            | None ->
              Utils.abort
                (Log.fatal "Ls_cmd_req: no CLI feedback sock for node %d"
                   ds_rank)
            | Some to_cli_sock ->
              let files_list = exec_ls_command detailed maybe_fn in
              let feedback = "" in
              (* ls output can be pretty big hence it is compressed *)
              send rng msg_counter local_node to_cli_sock
                (MDS_to_CLI (Ls_cmd_ack (files_list, feedback)))
          end
        | CLI_to_MDS Quit_cmd ->
          Log.debug "got Quit_cmd";
          let _ = Log.info "got Quit" in
          (* send Quit to all DSs *)
          IntMap.iter (fun i (_ds, maybe_sock, _maybe_cli_sock) ->
              match maybe_sock with
              | None -> Log.warn "DS %d missing" i
              | Some to_DS_i ->
                send rng msg_counter local_node to_DS_i (MDS_to_DS Quit_cmd)
            ) !int2node;
          Utils.nuke_file !machine_file;
          not_finished := false
    done;
    raise Types.Loop_end;
  with exn -> begin
      Socket_wrapper.nuke_keys ();
      Utils.nuke_CSPRNG rng;
      ZMQ.Socket.close incoming;
      let warn = true in
      Utils.cleanup_data_nodes warn !int2node;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end

let () = main ()

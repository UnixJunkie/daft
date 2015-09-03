open Batteries
open Legacy.Printf
open Types.Protocol

let mds_in_blue = Utils.fg_cyan ^ "MDS" ^ Utils.fg_reset

module A = Array
module Ht = Hashtbl
module FileSet = Types.FileSet
module File = Types.File
module Chunk = Types.File.Chunk
module L = List
module Logger = Log (* !!! keep this one before Log alias !!! *)
(* prefix all logs *)
module Log = Log.Make (struct let section = mds_in_blue end)
module Node = Types.Node

let global_state = ref FileSet.empty
let msg_counter = ref 0

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

let abort msg =
  Log.fatal "%s" msg;
  exit 1

let fetch_mds local_node fn ds_rank int2node feedback_to_cli =
  try
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
          begin match int2node.(chosen) with
            | (_node, Some to_ds_i, _maybe_cli_sock) ->
              let is_last = File.is_last_chunk chunk file in
              let send_order =
                MDS_to_DS
                  (Send_to_req (ds_rank, fn, chunk_id, is_last))
              in
              send msg_counter local_node to_ds_i send_order
            | (_, None, _) -> assert(false)
          end
      ) chunks
  with Not_found ->
    if feedback_to_cli then
      if Utils.out_of_bounds ds_rank int2node then
        Log.error "fetch_mds: fn: %s invalid ds_rank: %d"
          fn ds_rank
      else match Utils.trd3 int2node.(ds_rank) with
        | None ->
          Log.warn "fetch_mds: no CLI feedback sock for node %d" ds_rank
        | Some to_cli_sock ->
          send msg_counter local_node to_cli_sock (MDS_to_CLI (Fetch_cmd_nack fn))

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* setup MDS *)
  let port_in = ref Utils.default_mds_port_in in
  let machine_file = ref "" in
  Arg.parse
    [ "-p", Arg.Set_int port_in, "port where to listen";
      "-m", Arg.Set_string machine_file,
      "machine_file list of [user@]host:port (one per line)" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !machine_file = "" then abort "-m is mandatory";
  let int2node = Utils.data_nodes_array !machine_file in
  Log.info "read %d host(s)" (A.length int2node);
  start_data_nodes !machine_file;
  let hostname = Utils.hostname () in
  let local_node = Types.Node.create (-1) hostname !port_in None in
  (* start server *)
  Log.info "binding server to %s:%d" "*" !port_in;
  let ctx = ZMQ.Context.create () in
  let incoming = Utils.(zmq_socket Pull ctx "*" !port_in ) in
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
          if Utils.out_of_bounds ds_rank int2node then
            Log.warn "suspicious rank %d in Join_push" ds_rank
          else
            let expected_ds, prev_ds_sock, prev_cli_sock = int2node.(ds_rank) in
            begin match prev_ds_sock with
              | Some _ -> Log.warn "%s already joined" ds_as_string
              | None -> (* remember him for the future *)
                if ds = expected_ds then
                  let sock = Utils.(zmq_socket Push ctx ds_host ds_port_in) in
                  A.set int2node ds_rank (ds, Some sock, prev_cli_sock)
                else
                  Log.warn "suspicious Join req from %s" ds_as_string;
            end
        | DS_to_MDS (Add_file_req (ds_rank, f)) ->
          Log.debug "got Add_file_req";
          let open Types.File in
          if Utils.out_of_bounds ds_rank int2node then
            Log.warn "suspicious rank %d in Add_file_req" ds_rank
          else begin match Utils.snd3 int2node.(ds_rank) with
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
              send msg_counter local_node receiver (MDS_to_DS ack_or_nack)
          end
        | DS_to_MDS (Chunk_ack (fn, chunk_id, ds_rank)) ->
          begin
            Log.debug "got Chunk_ack";
            try
              (* 1) do we have this file ? *)
              let file = FileSet.find_fn fn !global_state in
              try
                (* 2) does it have this chunk ? *)
                let prev_chunk = File.find_chunk_id chunk_id file in
                (* 3) update global state: this chunk is owned by one more DS *)
                if Utils.out_of_bounds ds_rank int2node then
                  Log.warn
                    "invalid ds_rank in Chunk_ack: fn: %s chunk_id: %d \
                     ds_rank: %d" fn chunk_id ds_rank
                else
                  let new_source = Utils.fst3 int2node.(ds_rank) in
                  let new_chunk = Chunk.add_source prev_chunk new_source in
                  let new_file = File.update_chunk file new_chunk in
                  global_state := FileSet.update new_file !global_state
              with Not_found ->
                Log.error "Chunk_ack: unknown chunk: %s chunk_id: %d"
                  fn chunk_id
            with Not_found ->
              Log.error "Chunk_ack: unknown file: %s chunk_id: %d"
                fn chunk_id
          end
        | DS_to_MDS (Fetch_file_req (ds_rank, fn)) ->
	  Log.debug "got Fetch_file_req";
	  fetch_mds local_node fn ds_rank int2node true
        | DS_to_MDS (Bcast_file_req f) ->
          Log.debug "got Bcast_file_req";
          if FileSet.mem f !global_state then
            Log.warn "file already added: %s" (File.(f.name))
          else
            global_state := FileSet.add f !global_state
        | CLI_to_MDS (Connect_push (ds_rank, cli_port)) ->
          Log.debug "got Connect_push rank: %d port: %d" ds_rank cli_port;
          if Utils.out_of_bounds ds_rank int2node then
            Log.error "Connect_push: invalid ds_rank: %d" ds_rank
          else
            let node, ds_sock, maybe_cli_sock = int2node.(ds_rank) in
            assert(Option.is_some ds_sock); (* already a DS sock *)
            assert(Option.is_none maybe_cli_sock); (* not yet a CLI sock *)
            let cli_sock = Utils.(zmq_socket Push ctx (Node.get_host node) cli_port) in
            A.set int2node ds_rank (node, ds_sock, Some cli_sock)
        | CLI_to_MDS (Ls_cmd_req (ds_rank, detailed, maybe_fn)) ->
          Log.debug "got Ls_cmd_req";
          if Utils.out_of_bounds ds_rank int2node then
            Log.error "Ls_cmd_req: invalid ds_rank: %d" ds_rank
          else
            begin match Utils.trd3 int2node.(ds_rank) with
              | None ->
                abort
                  (sprintf "Ls_cmd_req: no CLI feedback sock for node %d" ds_rank)
              | Some to_cli_sock ->
                let files_list = exec_ls_command detailed maybe_fn in
                (* ls output can be pretty big hence it is compressed *)
                send ~compress:true msg_counter local_node to_cli_sock
                  (MDS_to_CLI (Ls_cmd_ack files_list))
            end
        | CLI_to_MDS Quit_cmd ->
          Log.debug "got Quit_cmd";
          let _ = Log.info "got Quit" in
          (* send Quit to all DSs *)
          A.iteri (fun i (_ds, maybe_sock, _maybe_cli_sock) -> match maybe_sock with
              | None -> Log.warn "DS %d missing" i
              | Some to_DS_i -> send msg_counter local_node to_DS_i (MDS_to_DS Quit_cmd)
            ) int2node;
          not_finished := false
    done;
    raise Types.Loop_end;
  with exn -> begin
      ZMQ.Socket.close incoming;
      let warn = true in
      Utils.cleanup_data_nodes_array warn int2node;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end

let () = main ()

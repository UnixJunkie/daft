open Batteries
open Printf
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
module Socket = Socket_wrapper.MDS_socket

let global_state = ref FileSet.empty
let cli_host = ref ""
let cli_port_in = ref Utils.default_cli_port_in

let start_data_nodes _machine_fn =
  (* TODO: scp exe to each node *)
  (* TODO: ssh node to start it *)
  ()

let abort msg =
  Log.fatal "%s" msg;
  exit 1

let fetch_mds fn ds_rank int2node to_cli=
  begin
    try
      let file = FileSet.find_fn fn !global_state in
      let chunks = Types.File.get_chunks file in
             (* FBR: code logic needs to moved into File 
                File.get_rand_sources will send a list of (chunk_rank, src_node) *)
             (* randomized algorithm: for each chunk we ask a randomly selected
                chunk source to send the chunk to destination *)
      Array.iter (fun chunk ->
        if Utils.out_of_bounds ds_rank int2node then
          Log.error "Fetch_cmd_req: fn: %s invalid ds_rank: %d"
            fn ds_rank
        else
          let selected_src_node = Chunk.select_source_rand chunk in
          let chosen = Node.get_rank selected_src_node in
          begin match int2node.(chosen) with
          | (_node, Some to_ds_i) ->
            let chunk_id = Chunk.get_id chunk in
            let is_last = File.is_last_chunk chunk file in
            let send_order =
              MDS_to_DS
                (Send_to_req (ds_rank, fn, chunk_id, is_last))
            in
            Socket.send to_ds_i send_order
          | (_, None) -> assert(false)
          end
      ) chunks
    with Not_found ->
      match to_cli with
      | None -> ()
      | Some sock ->
      (* this assumes there is a single CLI; might be wrong in future *)
	Socket.send  sock (MDS_to_CLI (Fetch_cmd_nack fn))
  end
    
(* better done with only one MDS -> local DS communication *)
let bcast_mds (fn: Types.filename) root int2node =
  Log.debug "coucou, tu veux voir mes bits ?";
  Array.iteri ( fun i _ -> 
    if i <> root then
      begin
	Log.debug "Envoi a DS %d" i;
	fetch_mds fn i int2node None
      end
  ) int2node

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
      "machine_file list of [user@]host:port (one per line)";
      "-cli", Arg.String (Utils.set_host_port cli_host cli_port_in),
      "<host:port> CLI" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !machine_file = "" then abort "-m is mandatory";
  if !cli_host = "" then abort "-cli is mandatory";
  if !cli_port_in = Utils.uninitialized then abort "-cli is mandatory";
  let int2node = Utils.data_nodes_array !machine_file in
  Log.info "read %d host(s)" (A.length int2node);
  start_data_nodes !machine_file;
  (* start server *)
  Log.info "binding server to %s:%d" "*" !port_in;
  let ctx = ZMQ.Context.create () in
  (* feedback socket to the local CLI *)
  let to_cli = Utils.(zmq_socket Push ctx !cli_host !cli_port_in) in
  let incoming = Utils.(zmq_socket Pull ctx "*" !port_in ) in
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      Log.debug "waiting msg";
      let message = Socket.receive incoming in
      Log.debug "got msg";
      begin match message with
       | DS_to_MDS (Join_push ds) ->
         Log.debug "got Join_push";
         let ds_as_string = Node.to_string ds in
         Log.info "DS %s Join req" ds_as_string;
         let ds_rank, ds_host, ds_port_in = Node.to_triplet ds in
         (* check it is the one we expect at that rank *)
         if Utils.out_of_bounds ds_rank int2node then
           Log.warn "suspicious rank %d in Join_push" ds_rank
         else
           let expected_ds, prev_sock = int2node.(ds_rank) in
           begin match prev_sock with
             | Some _ -> Log.warn "%s already joined" ds_as_string
             | None -> (* remember him for the future *)
               if ds = expected_ds then
                 let sock = Utils.(zmq_socket Push ctx ds_host ds_port_in) in
                 A.set int2node ds_rank (ds, Some sock)
               else
             Log.warn "suspicious Join req from %s" ds_as_string;
           end
       | DS_to_MDS (Add_file_req (ds_rank, f)) ->
         Log.debug "got Add_file_req";
         let open Types.File in
         if Utils.out_of_bounds ds_rank int2node then
           Log.warn "suspicious rank %d in Add_file_req" ds_rank
         else begin match snd int2node.(ds_rank) with
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
             Socket.send receiver (MDS_to_DS ack_or_nack)
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
                 let new_source = fst int2node.(ds_rank) in
                 let new_chunk = Chunk.add_source prev_chunk new_source in
                 File.update_chunk file new_chunk
             with Not_found ->
               Log.error "Chunk_ack: unknown chunk: %s chunk_id: %d"
                 fn chunk_id
           with Not_found ->
             Log.error "Chunk_ack: unknown file: %s chunk_id: %d"
               fn chunk_id
         end
       | DS_to_MDS (Fetch_file_req (ds_rank, fn)) ->
	 Log.debug "got Fetch_file_req";
	 fetch_mds fn ds_rank int2node ( Some to_cli )
       | DS_to_MDS (Bcast_file_req (ds_rank, fn)) ->
	 bcast_mds fn ds_rank int2node
       | CLI_to_MDS Ls_cmd_req ->
         Log.debug "got Ls_cmd_req";
         Socket.send to_cli
           (MDS_to_CLI (Ls_cmd_ack !global_state))
       | CLI_to_MDS Quit_cmd ->
         Log.debug "got Quit_cmd";
         let _ = Log.info "got Quit" in
         (* send Quit to all DSs *)
         A.iteri (fun i (_ds, maybe_sock) -> match maybe_sock with
             | None -> Log.warn "DS %d missing" i
             | Some to_DS_i -> Socket.send to_DS_i (MDS_to_DS Quit_cmd)
           ) int2node;
         not_finished := false
      end
    done;
    raise Types.Loop_end;
  with exn -> begin
      ZMQ.Socket.close incoming;
      ZMQ.Socket.close to_cli;
      let warn = true in
      Utils.cleanup_data_nodes_array warn int2node;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end
;;

main ()

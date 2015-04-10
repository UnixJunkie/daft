open Batteries
open Printf
open Types.Protocol

let mds_in_blue = Utils.fg_cyan ^ "MDS" ^ Utils.fg_reset

module A = Array
module Ht = Hashtbl
module FileSet = Types.FileSet
module L = List
module Logger = Log (* !!! keep this one before Log alias !!! *)
(* prefix all logs *)
module Log = Log.Make (struct let section = mds_in_blue end)
module Node = Types.Node
module Sock = ZMQ.Socket

let global_state = ref FileSet.empty
let cli_host = ref ""
let cli_port_in = ref Utils.default_cli_port_in

let start_data_nodes _machine_fn =
  (* TODO: scp exe to each node *)
  (* TODO: ssh node to start it *)
  ()

let abort msg =
  Log.fatal msg;
  exit 1

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
      let encoded = Sock.recv incoming in
      let message = decode encoded in
      Log.debug "got msg";
      begin match message with
       | DS_to_MDS (Join_push ds) -> (* ------------------------------------ *)
         Log.debug "got Join_push";
         let ds_as_string = Node.to_string ds in
         Log.info "DS %s Join req" ds_as_string;
         let ds_rank, ds_host, ds_port_in = Node.to_triplet ds in
         (* check it is the one we expect at that rank *)
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
       | DS_to_MDS (Add_file_req (ds_rank, f)) -> (* ----------------------- *)
         Log.debug "got Add_file_req";
         let open Types.File in
         begin match snd int2node.(ds_rank) with
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
             let answer = encode (MDS_to_DS ack_or_nack) in
             Sock.send receiver answer
         end
       | DS_to_MDS (Chunk_ack (_fn, _chunk)) -> (* ------------------------- *)
         Log.debug "got Chunk_ack";
         abort "Chunk_ack"
       | CLI_to_MDS Ls_cmd_req -> (* --------------------------------------- *)
         Log.debug "got Ls_cmd_req";
         let ls_ack = encode (MDS_to_CLI (Ls_cmd_ack !global_state)) in
         Sock.send to_cli ls_ack
       | CLI_to_MDS Quit_cmd -> (* ----------------------------------------- *)
         Log.debug "got Quit_cmd";
         let _ = Log.info "got Quit" in
         let quit = encode (MDS_to_DS Quit_cmd) in
         (* send Quit to all DSs *)
         A.iteri (fun i (_ds, maybe_sock) -> match maybe_sock with
             | None -> Log.warn "DS %d missing" i
             | Some to_DS_i -> Sock.send to_DS_i quit
           ) int2node;
         not_finished := false
       | DS_to_CLI  _ -> Log.warn "DS_to_CLI"
       | MDS_to_DS  _ -> Log.warn "MDS_to_DS"
       | MDS_to_CLI _ -> Log.warn "MDS_to_CLI"
       | DS_to_DS   _ -> Log.warn "DS_to_DS"
       | CLI_to_DS  _ -> Log.warn "CLI_to_DS"
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

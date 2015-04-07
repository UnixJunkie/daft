open Batteries
open Printf
open Types.Protocol

let mds_in_blue = Utils.fg_cyan ^ "MDS" ^ Utils.fg_reset

module A = Array
module Ht = Hashtbl
module L = List
module Logger = Log (* !!! keep this one before Log alias !!! *)
(* prefix all logs *)
module Log = Log.Make (struct let section = mds_in_blue end)
module Node = Types.Node
module Sock = ZMQ.Socket

let start_data_nodes () =
  (* FBR: scp exe to each node *)
  (* FBR: ssh node to start it *)
  failwith "not implemented yet"

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
  let host = Utils.hostname () in
  let machine_file = ref "" in
  Arg.parse
    [ "-p", Arg.Set_int port_in, "port where to listen";
      "-m", Arg.Set_string machine_file,
      "machine_file list of [user@]host:port (one per line)" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !machine_file = "" then abort "-m is mandatory";
  Log.info "MDS: %s:%d" host !port_in;
  let int2node = Utils.data_nodes_array !machine_file in
  Log.info "MDS: read %d host(s)" (A.length int2node);
  (* start all DSs *) (* FBR: later maybe, we can do this by hand for the moment *)
  (* start server *)
  Log.info "binding server to %s:%d" "*" !port_in;
  let ctx = ZMQ.Context.create () in
  let incoming = Utils.(zmq_socket Pull ctx "*" !port_in ) in
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      let encoded = Sock.recv incoming in
      let message = For_MDS.decode encoded in
      begin match message with
       | For_MDS.From_DS (Join_push ds) ->
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
       | For_MDS.From_DS (Chunk_ack (_fn, _chunk)) -> abort "Chunk_ack"
       | For_MDS.From_CLI Add_file_cmd_req _f -> abort "Add_file_cmd_req"
       | For_MDS.From_CLI Ls_cmd_req -> abort "Ls_cmd_req"
       | For_MDS.From_CLI Quit_cmd ->
         let _ = Log.info "got Quit" in
         (* send Quit to all DSs *)
         A.iteri (fun i (_ds, maybe_sock) -> match maybe_sock with
             | None -> Log.warn "DS %d missing" i
             | Some to_DS ->
               let quit = From_MDS.encode (From_MDS.To_DS Quit_cmd) in
               Sock.send to_DS quit
           ) int2node;
         not_finished := false
      end
    done
  with exn -> begin
      Log.error "exception";
      ZMQ.Socket.close incoming;
      A.iteri (fun i (_ds, maybe_sock) -> match maybe_sock with
          | Some s -> ZMQ.Socket.close s
          | None -> Log.warn "DS %d missing" i
        ) int2node;
      ZMQ.Context.terminate ctx;
      raise exn
    end
;;

main ()

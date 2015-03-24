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

let parse_machine_line (rank: int) (l: string): Node.t =
  let host, port = Utils.string_to_host_port l in
  Node.create rank host port

let parse_machine_file (fn: string): Node.t list =
  let res = ref [] in
  Utils.with_in_file fn
    (fun input ->
       try
         let i = ref 0 in
         while true do
           res := (parse_machine_line !i (Legacy.input_line input)) :: !res;
           incr i;
         done
       with End_of_file -> ()
    );
  L.rev !res

let data_nodes_array (fn: string) =
  let machines = parse_machine_file fn in
  let len = L.length machines in
  let res = A.create len (Node.dummy (), None) in
  L.iter (fun node -> A.set res (Node.get_rank node) (node, None)
         ) machines;
  res

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
  let port = ref Utils.default_mds_port in
  let host = Utils.hostname () in
  let machine_file = ref "" in
  Arg.parse
    [ "-p", Arg.Set_int port, "port where to listen";
      "-m", Arg.Set_string machine_file,
      "machine_file list of [user@]host:port (one per line)" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !machine_file = "" then abort "-m is mandatory";
  Log.info "MDS: %s:%d" host !port;
  let int2node = data_nodes_array !machine_file in
  Log.info "MDS: read %d host(s)" (A.length int2node);
  (* start all DSs *) (* FBR: later maybe, we can do this by hand for the moment *)
  (* start server *)
  Log.info "binding server to %s:%d" "*" !port;
  let ctx = ZMQ.Context.create () in
  let server_socket = Utils.zmq_server_setup ctx "*" !port in
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      let encoded_request = Sock.recv server_socket in
      let request = For_MDS.decode encoded_request in
      Log.debug "got req";
      begin match request with
       | For_MDS.From_DS (Join_req ds) ->
         let ds_as_string = Node.to_string ds in
         Log.info "DS %s Join req" ds_as_string;
         let ds_rank, ds_host, ds_port = Node.to_triplet ds in
         (* check it is the one we expect at that rank *)
         let expected_ds, prev_sock = int2node.(ds_rank) in
         begin match prev_sock with
         | Some _ -> Log.warn "%s already joined" ds_as_string
         | None -> (* remember him for the future *)
           if ds = expected_ds then
             let sock = Utils.zmq_client_setup ctx ds_host ds_port in
             A.set int2node ds_rank (ds, Some sock);
             let join_answer = From_MDS.(encode (To_DS Join_ack)) in
             Sock.send server_socket join_answer
           else
             let join_answer = From_MDS.(encode (To_DS Join_nack)) in
             Log.warn "suspicious Join req from %s" ds_as_string;
             Sock.send server_socket join_answer
         end
       | For_MDS.From_DS (Chunk_ack (_fn, _chunk)) -> abort "Chunk_ack"
       | For_MDS.From_CLI Add_file_cmd_req _f -> abort "Add_file_cmd_req"
       | For_MDS.From_CLI Ls_cmd_req -> abort "Ls_cmd_req"
       | For_MDS.From_CLI Quit_cmd_req ->
         let _ = Log.info "got Quit" in
         abort "Quit_cmd_req"
         (* FBR: send Quit to all DSs then Quit_Ack to the CLI
            FBR: this requires a push socket in the MDS and a pull docket
                 on each DS *)
         (* not_finished := false *)
      end
    done
  with exn ->
    (Log.error "exception";
     ZMQ.Socket.close server_socket;
     A.iteri (fun i (_ds, maybe_sock) -> match maybe_sock with
         | Some s -> ZMQ.Socket.close s
         | None -> Log.warn "DS %d missing" i
       ) int2node;
     ZMQ.Context.terminate ctx;
     raise exn)
;;

main ()

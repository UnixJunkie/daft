open Batteries
open Printf

module A = Array
module For_MDS = Types.Protocol.For_MDS
module L = List
module Logger = Log (* !!! keep this one before Log alias !!! *)
module Log = Log.Make(struct let section = "MDS" end) (* prefix logs *)
module Node = Types.Node
module Proto = Types.Protocol
module Sock = ZMQ.Socket

let parse_machine_line (rank: int) (l: string): Node.t =
  let hostname, port = String.split l ":" in
  Node.create rank hostname (int_of_string port)

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

let data_nodes_array (fn: string): Node.t array =
  let machines = parse_machine_file fn in
  let len = L.length machines in
  let res = A.create len (Node.dummy ()) in
  L.iter
    Node.(fun node -> A.set res node.rank node)
    machines;
  res

let start_data_nodes () =
  (* FBR: scp exe to each node *)
  (* FBR: ssh node to start it *)
  (* FBR: create a list of sockets for sending; one for each DS *)
  failwith "not implemented yet"

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
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !machine_file = "" then begin
    Log.fatal "-m is mandatory";
    exit 1;
  end;
  Log.info "MDS: %s:%d" host !port;
  let int2node = data_nodes_array !machine_file in
  Log.info "MDS: read %d hosts" (A.length int2node);
  (* start all DSs *) (* FBR: later maybe, we can do this by hand for the moment *)
  (* start server *)
  Log.info "binding server to %s:%d" "*" !port;
  let server_context, server_socket = Utils.zmq_server_setup "*" !port in
  (* loop on messages until quit command *)
  try
    let not_finished = ref true in
    while !not_finished do
      let encoded_request = Sock.recv server_socket in
      let request = Proto.For_MDS.of_string encoded_request in
      Log.info "got message";
      let open Proto in
      (match request with
       | For_MDS.From_DS (Join ds) ->
         (Log.info "DS %s joined" (Node.to_string ds);
          Sock.send server_socket "Join_OK")
       | _ ->
         Log.warn "unmanaged"
      );
      (* FBR: create a HT of sockets for DSs *)
    done;
  with exn ->
    (Log.error "exception";
     Utils.zmq_cleanup server_socket server_context;
     raise exn)
;;

main ()

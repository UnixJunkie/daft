open Batteries
open Printf

module A = Array
module L = List
module Logger = Log
module Log = Log.Make(struct let section = "MDS" end) (* prefix logs *)
module Node = Types.Node
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
  let context = ZMQ.Context.create () in
  let socket = Sock.create context Sock.rep in
  let port_str = string_of_int !port in
  let host_and_port = "tcp://*:" ^ port_str in (* FBR: something else than * doesn't work *)
  Log.info "binding to %s" host_and_port;
  Sock.bind socket host_and_port;
  (* loop on messages until quit command *)
  try
    let not_finished = ref true in
    while !not_finished do
      let _s = Sock.recv socket in
      Log.info "got message";
      Sock.send socket "OK";
      Log.info "sent answer";
      (* FBR: decode message *)
      (* FBR: pattern match on its type to process it *)
      Utils.sleep_ms 500; (* fake some work *)
      (* FBR: create a list of sockets for sending; one for each DS *)
    done;
  with exn -> begin
      Log.error "exception";
      Sock.close socket;
      ZMQ.Context.terminate context;
      raise exn;
    end
;;

main ()

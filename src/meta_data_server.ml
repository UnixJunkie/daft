open Batteries
open Printf

module A = Array
module L = List
module Logger = Log
module Log = Log.Make(struct let section = "MDS" end) (* prefix logs *)
module T = Types

let parse_machine_line (l: string): T.node =
  let hostname, port = String.split l ":" in
  T.create_node hostname (int_of_string port)

let parse_machine_file (fn: string): T.node list =
  let res = ref [] in
  Utils.with_in_file fn
    (fun input ->
       try
         while true do
           res := (parse_machine_line (Legacy.input_line input)) :: !res
         done
       with End_of_file -> ()
    );
  L.rev !res

let data_nodes_array (fn: string): T.node array =
  let machines = parse_machine_file fn in
  let len = L.length machines in
  let res = A.create len (T.create_node "" (-1)) in
  L.iteri
    (fun i node -> A.set res i node)
    machines;
  res

let start_data_nodes () =
  failwith "not implemented yet"

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* setup MDS *)
  let port = ref 0 in
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
  (* start all DSs *)
  failwith "not implemented yet"
  (* wait for commands *)
;;

main ()

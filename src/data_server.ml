open Batteries
open Printf

module Fn = Filename
module Logger = Log
module Log = Log.Make(struct let section = "DS" end) (* prefix all logs *)

(* module Client = Rpc_simple_client *)

(* create local data store with unix UGO rights 700 *)
let create_data_store (host: string) (port: int): string =
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root = sprintf "%s/%s:%d.ds" tmp_dir host port in
  Unix.mkdir data_store_root 0o700;
  Log.info "I store in %s" data_store_root;
  data_store_root

(* destroy a data store *)
let delete_data_store (ds: string): int =
  Sys.command ("rm -rf " ^ ds)

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* prefix all log messages *)

  (* setup data server *)
  let port = ref 0 in
  let host = Utils.hostname () in
  let server_name = ref "" in
  let server_port = ref 0 in
  (* options parsing *)
  Arg.parse
    [ "-p", Arg.Set_int port, "<port> where to listen";
      "-s", Arg.Set_string server_name, "<server> hostname";
      "-sp", Arg.Set_int server_port, "<port> server port" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  Log.info "Will connect to %s:%d" !server_name !server_port;
  let data_store = create_data_store host !port in
(*
  let connector = Rpc_client.Inet (server_name, server_port) in
  let protocol = Rpc.Tcp in
  let program = failwith "not implemented yet" in
  let client = Client.create connector protocol program in
  try
    while true do
      let delta = Client.call client "update_state" () in
      failwith "not implemented yet"
    done
  with exn ->
    begin
      Client.shutdown client;
      exit (delete_data_store data_store)
    end
*)
  exit (delete_data_store data_store)
;;

main ()

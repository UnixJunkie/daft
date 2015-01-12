open Batteries
open Printf

module Fn = Filename
module Logger = Log
module Log = Log.Make(struct let section = "DS" end) (* prefix logs *)

(* module Client = Rpc_simple_client *)

(* create local data store with unix UGO rights 700
   we take into account the port so that several DSs can be started on one
   host at the same time, for tests *)
let create_data_store (host: string) (port: int): string =
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root = sprintf "%s/%s:%d.ds" tmp_dir host port in
  Unix.mkdir data_store_root 0o700;
  Log.info "I store in %s" data_store_root;
  data_store_root

(* destroy a data store *)
let delete_data_store (ds: string): int =
  Sys.command ("rm -rf " ^ ds)

(* setup data server *)
let ds_log_fn = ref ""
let ds_host = Utils.hostname ()
let ds_port = ref 0
let mds_host = ref ""
let mds_port = ref (-1)

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-l", Arg.Set_string ds_log_fn, "<filename> where to log";
      "-p", Arg.Set_int ds_port, "<port> where to listen";
      "-s", Arg.Set_string mds_host, "<server> hostname";
      "-sp", Arg.Set_int mds_port, "<port> server port" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !ds_log_fn <> "" then begin (* don't log anything before that *)
    let log_out = Legacy.open_out !ds_log_fn in
    Logger.set_output log_out
  end;
  if !mds_host = "" then begin
    Log.fatal "-s is mandatory";
    exit 1;
  end;
  if !mds_port = -1 then begin
    Log.fatal "-sp is mandatory";
    exit 1;
  end;
  Log.info "Will connect to %s:%d" !mds_host !mds_port;
  let data_store = create_data_store !mds_host !mds_port in
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

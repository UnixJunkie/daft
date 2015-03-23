(* a CLI only talks to the MDS and to the local DS on the same node
   - all complex things should be handled by them and the CLI remain simple *)

open Batteries
open Printf
open Types.Protocol

module Fn = Filename
module From_DS = Types.Protocol.From_DS
module From_MDS = Types.Protocol.From_MDS
module FU = FileUtil
module Logger = Log
(* prefix all logs *)
module Log = Log.Make (struct let section = "CLI" end)
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet
module Sock = ZMQ.Socket

let uninitialized = -1

let ds_host = Utils.hostname ()
let ds_port = ref Utils.default_ds_port
let cli_port = ref Utils.default_cli_port
let mds_host = ref "localhost"
let mds_port = ref Utils.default_mds_port

let abort msg =
  Log.fatal msg;
  exit 1

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-mds", Arg.Set_string mds_host, "<server:port> MDS host";
      "-ds", Arg.Set_int mds_port, "<port> local DS port" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !mds_host = "" || !mds_port = uninitialized then abort "-mds is mandatory";
  if !ds_port = uninitialized then abort "-ds is mandatory";
  Log.info "Client of MDS %s:%d" !mds_host !mds_port;
  Log.info "binding server to %s:%d" "*" !ds_port;
  let mds_client_context, mds_client_socket = Utils.zmq_client_setup !mds_host !mds_port in
  let ds_client_context, ds_client_socket = Utils.zmq_client_setup "*" !ds_port in
  (* loop on user commands until quit command *)
  try
    let not_finished = ref true in
    while !not_finished do
      let command_str = read_line () in
      let parsed_command = BatString.nsplit ~by:" " command_str in
      Log.debug "got cmd";
      begin match parsed_command with
        | [] -> abort "parsed_command = []"
        | cmd :: _args ->
          begin match cmd with
            | "" -> Log.error "empty cmd"
            | "quit" ->
              let quit_cmd_req = For_MDS.encode (For_MDS.From_CLI (Quit_cmd_req)) in
              Sock.send mds_client_socket quit_cmd_req;
              let encoded_answer = Sock.recv mds_client_socket in
              let answer = From_MDS.decode encoded_answer in
              assert(answer = From_MDS.To_CLI Quit_cmd_ack);
              Log.info "quit ack";
            | _ -> Log.error "unhandled: %s" cmd
          end
      end
    done;
  with exn -> begin
      Log.info "exception";
      Utils.zmq_cleanup mds_client_context mds_client_socket;
      Utils.zmq_cleanup ds_client_context ds_client_socket;
      raise exn;
    end
;;

main ()

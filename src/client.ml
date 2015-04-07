(* a CLI only talks to the MDS and to the local DS on the same node
   - all complex things should be handled by them and the CLI remain simple *)

open Batteries
open Printf
open Types.Protocol

module Fn = Filename
module FU = FileUtil
module Logger = Log
module Log = Log.Make (struct let section = "CLI" end) (* prefix all logs *)
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet
module Sock = ZMQ.Socket

let uninitialized = -1

let ds_host = ref (Utils.hostname ())
let ds_port_in = ref Utils.default_ds_port_in
let mds_host = ref "localhost"
let mds_port_in = ref Utils.default_mds_port_in

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
    [ "-mds", Arg.String (Utils.set_host_port mds_host mds_port_in),
      "<host:port> MDS";
      "-ds", Arg.String (Utils.set_host_port ds_host ds_port_in),
      "<host:port> local DS" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !mds_host = "" || !mds_port_in = uninitialized then abort "-mds is mandatory";
  if !ds_host  = "" || !ds_port_in  = uninitialized then abort "-ds is mandatory";
  let ctx = ZMQ.Context.create () in
  let for_MDS = Utils.(zmq_socket Push ctx !mds_host !mds_port_in) in
  Log.info "Client of MDS %s:%d" !mds_host !mds_port_in;
  let for_DS  = Utils.(zmq_socket Push ctx !ds_host  !ds_port_in ) in
  Log.info "Client of DS %s:%d" !ds_host !ds_port_in;
  (* the CLI execute just one command then exit *)
  (* we could have a batch mode, executing several commands from a file *)
  let not_finished = ref true in
  try
    while !not_finished do
      let command_str = read_line () in
      Log.info "command: %s" command_str;
      let parsed_command = BatString.nsplit ~by:" " command_str in
      begin match parsed_command with
        | [] -> Log.error "empty command"
        | cmd :: _args ->
          begin match cmd with
            | "" -> Log.error "cmd = \"\""
            | "quit" ->
              let quit_cmd = For_MDS.encode (For_MDS.From_CLI (Quit_cmd)) in
              Sock.send for_MDS quit_cmd;
              not_finished := false;
            | _ -> Log.error "unknown command: %s" cmd
          end
      end
    done
  with exn -> begin
      Log.info "exception";
      ZMQ.Socket.close for_MDS;
      ZMQ.Socket.close for_DS;
      ZMQ.Context.terminate ctx;
      raise exn
    end
;;

main ()

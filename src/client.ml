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
let cli_port_in = ref Utils.default_cli_port_in

let abort msg =
  Log.fatal msg;
  exit 1

let process_answer incoming maybe_cont =
  Log.debug "waiting msg";
  let encoded = Sock.recv incoming in
  let message = decode encoded in
  Log.debug "got msg";
  match message with
  | MDS_to_CLI (Ls_cmd_ack f) ->
    Log.debug "got Ls_cmd_ack";
    let listing = FileSet.to_string f in
    Log.info "%s" listing
  | MDS_to_CLI (Fetch_cmd_nack fn) ->
    Log.debug "got Fetch_cmd_nack";
    Log.error "no such file: %s" fn
  | DS_to_CLI (Fetch_file_cmd_ack fn) ->
    begin
      Log.debug "got Fetch_file_cmd_ack";
      Log.info "%s: OK" fn;
      match maybe_cont with
      | None -> ()
      | Some continuation -> continuation ()
    end
  | DS_to_CLI (Fetch_file_cmd_nack (fn, err)) ->
    Log.debug "got Fetch_file_cmd_nack";
    Log.error "%s: %s" fn (string_of_error err)
  | DS_to_MDS _ -> Log.warn "DS_to_MDS"
  | MDS_to_DS _ -> Log.warn "MDS_to_DS"
  | DS_to_DS _ -> Log.warn "DS_to_DS"
  | CLI_to_MDS _ -> Log.warn "CLI_to_MDS"
  | CLI_to_DS _ -> Log.warn "CLI_to_DS"

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-cli", Arg.Set_int cli_port_in, "<port> where the CLI is listening";
      "-mds", Arg.String (Utils.set_host_port mds_host mds_port_in),
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
  let for_DS = Utils.(zmq_socket Push ctx !ds_host !ds_port_in) in
  let incoming = Utils.(zmq_socket Pull ctx "*" !cli_port_in) in
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
        | cmd :: args ->
          begin match cmd with
            | "" -> Log.error "cmd = \"\""
            | "put"
            | "fetch" ->
              begin match args with
                | [] -> Log.error "no filename"
                | [fn] ->
                  let f_loc = match cmd with
                    | "put" -> Local
                    | "fetch" -> Remote
                    | _ -> assert(false)
                  in
                  let put = encode (CLI_to_DS (Fetch_file_cmd_req (fn, f_loc))) in
                  Sock.send for_DS put;
                  process_answer incoming None
                | _ -> Log.error "more than one filename"
              end
            | "extract" ->
              begin match args with
                | [] -> Log.error "no filename"
                | [src_fn; dst_fn] ->
                  let extract = encode (CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn))) in
                  Sock.send for_DS extract;
                  process_answer incoming None
                | _ -> Log.error "too many filenames"
              end
            | "q" | "quit" | "exit" ->
              let quit_cmd = encode (CLI_to_MDS Quit_cmd) in
              Sock.send for_MDS quit_cmd;
              not_finished := false;
            | "l" | "ls" ->
              let ls_cmd = encode (CLI_to_MDS Ls_cmd_req) in
              Sock.send for_MDS ls_cmd;
              process_answer incoming None
            | _ -> Log.error "unknown command: %s" cmd
          end
      end
    done;
    raise Types.Loop_end;
  with exn -> begin
      ZMQ.Socket.close for_MDS;
      ZMQ.Socket.close for_DS;
      ZMQ.Socket.close incoming;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end
;;

main ()

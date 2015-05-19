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

let ds_host = ref ""
let ds_port_in = ref uninitialized
let mds_host = ref ""
let mds_port_in = ref uninitialized
let cli_port_in = ref Utils.default_cli_port_in
let do_compress = ref false

let abort msg =
  Log.fatal "%s" msg;
  exit 1

let do_nothing () =
  ()

let getenv_or_fail variable_name =
  try Sys.getenv variable_name
  with Not_found ->
    Log.error "getenv_or_fail: Sys.getenv: %s" variable_name;
    ""

let process_answer incoming continuation =
  Log.debug "waiting msg";
  let encoded = Sock.recv incoming in
  let message = decode !do_compress encoded in
  Log.debug "got msg";
  match message with
  | MDS_to_CLI (Ls_cmd_ack f) ->
    Log.debug "got Ls_cmd_ack";
    let listing = FileSet.to_string f in
    Log.info "\n%s" listing
  | MDS_to_CLI (Fetch_cmd_nack fn) ->
    Log.debug "got Fetch_cmd_nack";
    Log.error "no such file: %s" fn
  | DS_to_CLI (Fetch_file_cmd_ack fn) ->
    begin
      Log.debug "got Fetch_file_cmd_ack";
      Log.info "%s: OK" fn;
      continuation ()
    end
  | DS_to_CLI (Fetch_file_cmd_nack (fn, err)) ->
    Log.debug "got Fetch_file_cmd_nack";
    Log.error "%s: %s" fn (string_of_error err)
  | DS_to_MDS _ -> Log.warn "DS_to_MDS"
  | MDS_to_DS _ -> Log.warn "MDS_to_DS"
  | DS_to_DS _ -> Log.warn "DS_to_DS"
  | CLI_to_MDS _ -> Log.warn "CLI_to_MDS"
  | CLI_to_DS _ -> Log.warn "CLI_to_DS"

let get_one: 'a list -> 'a option = function
  | [x] -> Some x
  | _ -> None

let get_two: 'a list -> ('a * 'a) option = function
  | [x; y] -> Some (x, y)
  | _ -> None

module Command = struct
  type filename = string
  type t = Put of filename
         | Get of filename * filename (* src_fn dst_fn *)
         | Fetch of filename
         | Rfetch of filename * string (* string format: host:port *)
         | Extract of filename * filename
         | Quit
         | Ls
         | Skip (* do nothing *)
  (* understand a command as soon as it is unambiguous; quick and dirty *)
  let of_string: string list -> t = function
    | [] -> Skip
    | cmd :: args ->
      begin match cmd with
        | "p" | "pu" | "put" ->
          begin match get_one args with
            | Some fn -> Put fn
            | None -> Log.error "\nusage: put fn" ; Skip
          end
        | "g" | "ge" | "get" ->
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Get (src_fn, dst_fn)
            | None -> Log.error "\nusage: get src_fn dst_fn" ; Skip
          end
        | "f" | "fe" | "fet" | "fetc" | "fetch" ->
          begin match get_one args with
            | Some fn -> Fetch fn
            | None -> Log.error "\nusage: fetch fn" ; Skip
          end
        | "r" | "rf" | "rfe" | "rfet" | "rfetc" | "rfetch" ->
          begin match get_two args with
            | Some (src_fn, host_port) -> Rfetch (src_fn, host_port)
            | None -> Log.error "\nusage: rfetch fn host:port" ; Skip
          end
        | "e" | "ex" | "ext" | "extr" | "extra" | "extrac" | "extract" ->
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Extract (src_fn, dst_fn)
            | None -> Log.error "\nusage: extract src_fn dst_fn" ; Skip
          end
        | "q" | "qu" | "qui" | "quit" -> Quit
        | "l" | "ls" -> Ls
        | "" -> Log.error "empty command"; Skip
        | cmd -> Log.error "unknown command: %s" cmd; Skip
      end
end

let extract_cmd src_fn dst_fn for_DS incoming =
  let extract = encode !do_compress (CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn))) in
  Sock.send for_DS extract;
  process_answer incoming do_nothing

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
      "<host:port> local DS";
      "-z", Arg.Set do_compress, " enable on the fly compression" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !mds_host = "" && !mds_port_in = uninitialized then
    begin
      let daft_mds_env_var = getenv_or_fail "DAFT_MDS" in
      if daft_mds_env_var = "" then
        abort "-mds option or DAFT_MDS env. var. mandatory"
      else
        Utils.set_host_port mds_host mds_port_in daft_mds_env_var
    end;
  assert(!mds_host <> "" && !mds_port_in <> uninitialized);
  if !ds_host = "" && !ds_port_in = uninitialized then
    begin
      let daft_ds_env_var = getenv_or_fail "DAFT_DS" in
      if daft_ds_env_var = "" then
        abort "-ds option or DAFT_DS env. var. mandatory"
      else
        Utils.set_host_port ds_host ds_port_in daft_ds_env_var
    end;
  assert(!ds_host <> "" && !ds_port_in <> uninitialized);
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
      let open Command in
      let command_str = read_line () in
      Log.debug "command: '%s'" command_str;
      let command_line = BatString.nsplit ~by:" " command_str in
      let cmd = Command.of_string command_line in
      match cmd with
      | Skip -> Log.info "\nusage: put|get|fetch|rfetch|extract|quit|ls"
      | Put src_fn ->
        let put = encode !do_compress (CLI_to_DS (Fetch_file_cmd_req (src_fn, Local))) in
        Sock.send for_DS put;
        process_answer incoming do_nothing
      | Get (src_fn, dst_fn) ->
        (* get = extract . fetch *)
        let get = encode !do_compress (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote))) in
        Sock.send for_DS get;
        let fetch_cont = (fun () -> extract_cmd src_fn dst_fn for_DS incoming) in
        process_answer incoming fetch_cont
      | Fetch src_fn ->
        let fetch = encode !do_compress (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote))) in
        Sock.send for_DS fetch;
        process_answer incoming do_nothing
      | Rfetch (src_fn, host_port) ->
        let put = encode !do_compress (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote))) in
        let host, port = ref "", ref 0 in
        Utils.set_host_port host port host_port;
        (* create temp socket to remote DS *)
        let for_ds_i = Utils.(zmq_socket Push ctx !host !port) in
        Sock.send for_ds_i put;
        process_answer incoming do_nothing;
        ZMQ.Socket.close for_ds_i
      | Extract (src_fn, dst_fn) ->
        extract_cmd src_fn dst_fn for_DS incoming
      | Quit ->
        let quit_cmd = encode !do_compress (CLI_to_MDS Quit_cmd) in
        Sock.send for_MDS quit_cmd;
        not_finished := false;
      | Ls ->
        let ls_cmd = encode !do_compress (CLI_to_MDS Ls_cmd_req) in
        Sock.send for_MDS ls_cmd;
        process_answer incoming do_nothing
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

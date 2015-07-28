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
module Socket = Socket_wrapper.CLI_socket

let ds_host = ref ""
let ds_port_in = ref Utils.uninitialized
let mds_host = ref ""
let mds_port_in = ref Utils.uninitialized
let cli_port_in = ref Utils.uninitialized
let single_command = ref ""
let interactive = ref false
let machine_file = ref ""

(* FBR: pass the Log module around if that's possible *)

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

let process_answer incoming continuation: unit =
  Log.debug "waiting msg";
  let message' = Socket.receive incoming in
  Log.debug "got msg";
  match message' with
  | None -> Log.warn "junk"
  | Some message ->
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
    | DS_to_CLI Bcast_file_ack ->
      begin
        Log.debug "got Bcast_file_ack";
        continuation ()
      end
    | DS_to_CLI (Fetch_file_cmd_nack (fn, err)) ->
      Log.debug "got Fetch_file_cmd_nack";
      Log.error "%s: %s" fn (string_of_error err)

let get_one: 'a list -> 'a option = function
  | [x] -> Some x
  | _ -> None

let get_two: 'a list -> ('a * 'a) option = function
  | [x; y] -> Some (x, y)
  | _ -> None

module Command = struct
  type filename = string
  type port = int
  type t = Bcast of filename
         | Exit (* just exit CLI *)
         | Extract of filename * filename
         | Fetch of filename
         | Get of filename * filename (* src_fn dst_fn *)
         | Ls
         | Put of filename
         | Quit (* turn off whole system *)
         | Rfetch of filename * string (* string format: host:port *)
         | Skip
  (* quick and dirty way to understand a command ASAP *)
  let of_list: string list -> t = function
    | [] -> Skip
    | cmd :: args ->
      begin match cmd with
        | "b" | "bcast" ->
          begin match get_one args with
            | Some fn -> Bcast fn
            | None -> Log.error "\nusage: bcast fn" ; Skip
          end
        | "e" | "extract" ->
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Extract (src_fn, dst_fn)
            | None -> Log.error "\nusage: extract src_fn dst_fn" ; Skip
          end
        | "f" | "fetch" ->
          begin match get_one args with
            | Some fn -> Fetch fn
            | None -> Log.error "\nusage: fetch fn" ; Skip
          end
        | "g" | "get" ->
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Get (src_fn, dst_fn)
            | None -> Log.error "\nusage: get src_fn dst_fn" ; Skip
          end
        | "l" | "ls" -> Ls
        | "p" | "put" ->
          begin match get_one args with
            | Some fn -> Put fn
            | None -> Log.error "\nusage: put fn" ; Skip
          end
        | "q" | "quit" -> Quit
        | "r" | "rfetch" ->
          begin match get_two args with
            | Some (src_fn, host_port) -> Rfetch (src_fn, host_port)
            | None -> Log.error "\nusage: rfetch fn host:port" ; Skip
          end
        | "x" | "exit" -> Exit
        | "" -> Log.error "empty command"; Skip
        | cmd -> Log.error "unknown command: %s" cmd; Skip
      end
end

let extract_cmd local_node src_fn dst_fn for_DS incoming =
  Socket.send local_node for_DS
    (CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn)));
  process_answer incoming do_nothing

let read_one_command is_interactive =
  let command_line =
    let command_str =
      if is_interactive then read_line ()
      else !single_command
    in
    Log.debug "command: '%s'" command_str;
    BatString.nsplit ~by:" " command_str
  in
  Command.of_list command_line

let main () =
  let my_rank = ref Utils.uninitialized in
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-i", Arg.Set interactive, " interactive mode of the CLI";
      "-c", Arg.Set_string single_command,
      "'command' execute a single command; use quotes if several words";
      "-p", Arg.Set_int cli_port_in, "<port> where the CLI is listening";
      "-mds", Arg.String (Utils.set_host_port mds_host mds_port_in),
      "<host:port> MDS";
      "-ds", Arg.String (Utils.set_host_port ds_host ds_port_in),
      "<host:port> local DS" ;
      "-r", Arg.Set_int my_rank, "<rank> rank of the local data node" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !my_rank = Utils.uninitialized then abort "-r is mandatory";
  if !cli_port_in = Utils.uninitialized then abort "-p is mandatory";
  if !mds_host = "" && !mds_port_in = Utils.uninitialized then
    begin
      let daft_mds_env_var = getenv_or_fail "DAFT_MDS" in
      if daft_mds_env_var = "" then
        abort "-mds option or DAFT_MDS env. var. mandatory"
      else
        Utils.set_host_port mds_host mds_port_in daft_mds_env_var
    end;
  assert(!mds_host <> "" && !mds_port_in <> Utils.uninitialized);
  if !ds_host = "" && !ds_port_in = Utils.uninitialized then
    begin
      let daft_ds_env_var = getenv_or_fail "DAFT_DS" in
      if daft_ds_env_var = "" then
        abort "-ds option or DAFT_DS env. var. mandatory"
      else
        Utils.set_host_port ds_host ds_port_in daft_ds_env_var
    end;
  assert(!ds_host <> "" && !ds_port_in <> Utils.uninitialized);
  let hostname = Utils.hostname () in
  (* local_node does not correspond to a DS; it is initialized dirtily *)
  let local_node = Node.create Utils.uninitialized hostname !cli_port_in None in
  let ctx = ZMQ.Context.create () in
  let for_MDS = Utils.(zmq_socket Push ctx !mds_host !mds_port_in) in
  Log.info "Client of MDS %s:%d" !mds_host !mds_port_in;
  let for_DS = Utils.(zmq_socket Push ctx !ds_host !ds_port_in) in
  (* register yourself to the local DS by telling it the port you listen to *)
  Socket.send local_node for_DS (CLI_to_DS (Connect_cmd_push !cli_port_in));
  (* register yourself to the MDS *)
  Socket.send local_node for_MDS
    (CLI_to_MDS (Connect_push (!my_rank, !cli_port_in)));
  let incoming = Utils.(zmq_socket Pull ctx "*" !cli_port_in) in
  Log.info "Client of DS %s:%d" !ds_host !ds_port_in;
  (* the CLI execute just one command then exit *)
  (* we could have a batch mode, executing several commands from a file *)
  let not_finished = ref true in
  try
    while !not_finished do
      not_finished := !interactive;
      let open Command in
      match read_one_command !interactive with
      | Skip -> Log.info "\nusage: put|get|fetch|rfetch|extract|exit|quit|ls|bcast"
      | Put src_fn ->
        Socket.send local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Local)));
        process_answer incoming do_nothing
      | Get (src_fn, dst_fn) ->
        (* get = extract . fetch *)
        Socket.send local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        let fetch_cont =
          (fun () -> extract_cmd local_node src_fn dst_fn for_DS incoming) 
        in
        process_answer incoming fetch_cont
      | Fetch src_fn ->
        Socket.send local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        process_answer incoming do_nothing
      | Rfetch (src_fn, host_port) ->
        let host, port = ref "", ref 0 in
        Utils.set_host_port host port host_port;
        (* create temp socket to remote DS *)
        let for_ds_i = Utils.(zmq_socket Push ctx !host !port) in
        Socket.send local_node for_ds_i
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        process_answer incoming do_nothing;
        ZMQ.Socket.close for_ds_i
      | Extract (src_fn, dst_fn) ->
        extract_cmd local_node src_fn dst_fn for_DS incoming
      | Quit ->
        Socket.send local_node for_MDS
          (CLI_to_MDS Quit_cmd);
        not_finished := false;
      | Exit ->
        not_finished := false
      | Ls ->
        Socket.send local_node for_MDS (CLI_to_MDS (Ls_cmd_req !my_rank));
        process_answer incoming do_nothing
      | Bcast src_fn ->
        Socket.send local_node for_DS
          (CLI_to_DS (Bcast_file_cmd_req src_fn));
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

let () = main ()

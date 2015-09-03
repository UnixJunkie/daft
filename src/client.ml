(* a CLI only talks to the MDS and to the local DS on the same node
   - all complex things should be handled by them and the CLI remain simple *)

open Batteries
open Legacy.Printf
open Types.Protocol

module Fn = Filename
module FU = FileUtil
module Logger = Log
module Log = Log.Make (struct let section = "CLI" end) (* prefix all logs *)
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet

let send, receive = Socket_wrapper.CLI_socket.(send, receive)

let ds_host = ref ""
let ds_port_in = ref Utils.uninitialized
let mds_host = ref ""
let mds_port_in = ref Utils.uninitialized
let cli_port_in = ref Utils.uninitialized
let single_command = ref ""
let interactive = ref false
let machine_file = ref ""
let verbose = ref false
let msg_counter = ref 0
let msg_counter_fn = "CLI.msg_counter"

(* FBR: pass the Log module around if that's possible *)

let backup_counter () =
  Utils.with_out_file msg_counter_fn (fun out ->
      fprintf out "%d\n" !msg_counter
    );
  Unix.chmod msg_counter_fn 0o600; (* strict perms *)
  Log.info "wrote out %s" msg_counter_fn

let restore_counter () =
  if Sys.file_exists msg_counter_fn then
    begin
      Log.info "read in %s" msg_counter_fn;
      Utils.with_in_file msg_counter_fn (fun input ->
          int_of_string (Legacy.input_line input)
        )
    end
  else
    0 (* start from fresh *)

let forget_counter () =
  if Sys.file_exists msg_counter_fn then
    begin
      FU.(rm ~force:Force [msg_counter_fn]);
      Log.info "removed %s" msg_counter_fn
    end

let abort msg =
  Log.fatal "%s" msg;
  exit 1

let do_nothing () =
  ()

let process_answer incoming continuation: unit =
  Log.debug "waiting msg";
  let message' = receive incoming in
  Log.debug "got msg";
  match message' with
  | None -> Log.warn "junk"
  | Some message ->
    match message with
    | MDS_to_CLI (Ls_cmd_ack f) ->
      Log.debug "got Ls_cmd_ack";
      let listing = FileSet.to_string f in
      Printf.printf "%s\n" listing
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
  type t = Bcast of filename * bcast_method (* FBR: bcast needs an optional dst_fn *)
         | Exit (* to leave the CLI just temporarily *)
         | Extract of filename * filename
         | Fetch of filename
         | Get of filename * filename (* src_fn dst_fn *) (* FBR: dst_fn should be optional *)
         | Ls of bool * filename option (* ls [-a] [filename] *)
         | Put of filename (* FBR: put needs an optional dst_fn *)
         | Quit (* turn off whole system *)
         | Skip
  let usage () =
    Log.info
      ("\n" ^^
       "usage: put|get|help|fetch|extract|exit|quit|ls|bcast\n" ^^
       "hotkey ^   ^   ^    ^     ^        ^   ^    ^  ^\n" ^^
       "ls [-a] [filename]")
      (* FBR: give a whole listing of commands with parameters *)
  let bcast_of_string = function
    | "r" -> Relay
    | "a" -> Amoeba
    | "w" -> Well_exhaust
    | x ->
      let err_msg =
        sprintf "broadcast_method: unsupported: %s (supported: s|r|a)" x
      in
      failwith err_msg
  (* quick and dirty way to understand a command ASAP *)
  let of_list: string list -> t = function
    | [] -> Skip
    | cmd :: args ->
      begin match cmd with
        | "b" | "bcast" ->
          begin match get_two args with
            | Some (fn, bcast_method) ->
              Bcast (fn, bcast_of_string bcast_method)
            | None -> Log.error "\nusage: bcast fn {r|a|w}" ; Skip
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
        | "h" | "help" -> usage(); Skip
        | "l" | "ls" ->
          begin match get_two args with
            | Some (_, fn) -> Ls (true, Some fn)
            | None ->
              begin match get_one args with
                | Some str ->
                  if str = "-a" then Ls (true, None)
                  else Ls (false, Some str)
                | None -> Ls (false, None)
              end
          end
        | "p" | "put" ->
          begin match get_one args with
            | Some fn -> Put fn
            | None -> Log.error "\nusage: put fn" ; Skip
          end
        | "q" | "quit" -> Quit
        | "x" | "exit" -> Exit
        | "" ->  Log.error "empty command";           usage(); Skip
        | cmd -> Log.error "unknown command: %s" cmd; usage(); Skip
      end
end

let extract_cmd local_node src_fn dst_fn for_DS incoming =
  send msg_counter local_node for_DS
    (CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn)));
  process_answer incoming do_nothing

let read_one_command is_interactive =
  let before, command_line =
    let command_str =
      if is_interactive then
        begin
          printf "\027[1;31m> \027[0m"; (* bold-red prompt *)
          read_line ()
        end
      else !single_command
    in
    let before = Unix.gettimeofday () in
    Log.debug "command: '%s'" command_str;
    let to_parse =
      if String.starts_with command_str "!" then
        (* commands starting with ! are passed to the shell
           like with any good old FTP client *)
        let (_: Unix.process_status) =
          Unix.system (String.lchop command_str)
        in
        []
      else
        match String.nsplit ~by:" " command_str with
        | [] -> [""]
        | x -> x
    in
    (before, to_parse)
  in
  (before, Command.of_list command_line)

let main () =
  let my_rank = ref Utils.uninitialized in
  (* setup logger *)
  Logger.set_log_level Logger.INFO;
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
      "-r", Arg.Set_int my_rank, "<rank> rank of the local data node";
      "-v", Arg.Set verbose, " verbose mode"]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !verbose then Logger.set_log_level Logger.DEBUG;
  if !my_rank = Utils.uninitialized then abort "-r is mandatory";
  if !cli_port_in = Utils.uninitialized then abort "-p is mandatory";
  if !mds_host = "" && !mds_port_in = Utils.uninitialized then
    begin
      let daft_mds_env_var = Utils.getenv_or_fail "DAFT_MDS" in
      if daft_mds_env_var = "" then
        abort "-mds option or DAFT_MDS env. var. mandatory"
      else
        Utils.set_host_port mds_host mds_port_in daft_mds_env_var
    end;
  assert(!mds_host <> "" && !mds_port_in <> Utils.uninitialized);
  if !ds_host = "" && !ds_port_in = Utils.uninitialized then
    begin
      let daft_ds_env_var = Utils.getenv_or_fail "DAFT_DS" in
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
  (* continue from a previous session if counter file is found *)
  msg_counter := restore_counter ();
  if !msg_counter = 0 then (* start a new session *)
    begin
      (* register yourself to the local DS by telling it the port you listen to *)
      send msg_counter local_node for_DS (CLI_to_DS (Connect_cmd_push !cli_port_in));
      (* register yourself to the MDS *)
      send msg_counter local_node for_MDS
        (CLI_to_MDS (Connect_push (!my_rank, !cli_port_in)))
    end;
  let incoming = Utils.(zmq_socket Pull ctx "*" !cli_port_in) in
  Log.info "Client of DS %s:%d" !ds_host !ds_port_in;
  (* the CLI execute just one command then exit *)
  (* we could have a batch mode, executing several commands from a file *)
  let not_finished = ref true in
  try
    while !not_finished do
      not_finished := !interactive;
      let open Command in
      let before, cmd = read_one_command !interactive in
      match cmd with
      | Skip -> ()
      | Put src_fn ->
        send msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Local)));
        process_answer incoming do_nothing
      | Get (src_fn, dst_fn) ->
        (* get = extract . fetch *)
        send msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        let fetch_cont =
          (fun () -> extract_cmd local_node src_fn dst_fn for_DS incoming) 
        in
        process_answer incoming fetch_cont;
        if !verbose then
          begin
            let after = Unix.gettimeofday () in
            Log.info "%.3f" (after -. before)
          end
      | Fetch src_fn ->
        send msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        process_answer incoming do_nothing
      | Extract (src_fn, dst_fn) ->
        extract_cmd local_node src_fn dst_fn for_DS incoming
      | Quit ->
        send msg_counter local_node for_MDS (CLI_to_MDS Quit_cmd);
        forget_counter ();
        not_finished := false
      | Exit ->
        backup_counter ();
        not_finished := false
      | Ls (detailed, maybe_fn) ->
        send msg_counter local_node for_MDS
          (CLI_to_MDS (Ls_cmd_req (!my_rank, detailed, maybe_fn)));
        process_answer incoming do_nothing
      | Bcast (src_fn, bcast_method) ->
        send msg_counter local_node for_DS
          (CLI_to_DS (Bcast_file_cmd_req (src_fn, bcast_method)));
        process_answer incoming do_nothing;
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

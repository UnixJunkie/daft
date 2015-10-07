(* a CLI only talks to the MDS and to the local DS on the same node
   - all complex things should be handled by them and the CLI remain simple *)

open Batteries
open Legacy.Printf
open Types.Protocol

module Fn = Filename
module FU = FileUtil
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet

let send, receive = Socket_wrapper.CLI_socket.(send, receive)

let cli_port_in = ref 8000 (* default CLI listening port *)
let single_command = ref ""
let interactive = ref false
let machine_file = ref ""
let verbose = ref false
let msg_counter = ref 0
let msg_counter_fn = "CLI.msg_counter"
let rng = Utils.create_CSPRNG ()

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
    | MDS_to_CLI (Ls_cmd_ack (f, feedback)) ->
      Log.debug "got Ls_cmd_ack";
      let listing = FileSet.to_string f in
      Printf.printf "%s\n" listing;
      if feedback <> "" then Printf.printf "%s\n" feedback
    | MDS_to_CLI (Fetch_cmd_nack fn) ->
      Log.debug "got Fetch_cmd_nack";
      Log.error "no such file: %s" fn
    | MDS_to_CLI Unlock ->
      continuation ()
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
  (* quick and dirty way to understand a command ASAP *)
  let of_list: string list -> t = function
    | [] | ["skip"] -> Skip
    | cmd :: args ->
      begin match cmd with
        | "b" | "bcast" ->
          begin match get_two args with
            | Some (fn, bcast_method) ->
              Bcast (fn, Utils.bcast_of_string bcast_method)
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
                  if str = "-l" then Ls (true, None)
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
  send rng msg_counter local_node for_DS
    (CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn)));
  process_answer incoming do_nothing

let read_one_command is_interactive =
  let before, command_line =
    let command_str =
      if is_interactive then
        begin
          printf "\027[1;31m> \027[0m"; (* bold-red prompt *)
          try
            read_line ()
          with End_of_file -> "x" (* default to exit command *)
        end
      else !single_command
    in
    let before = Unix.gettimeofday () in
    Log.debug "command: '%s'" command_str;
    let to_parse =
      if String.starts_with command_str "#" then
        ["skip"] (* comment *)
      else if String.starts_with command_str "!" then
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

(* recursive ls *)
let ls dir_name =
  let rec loop acc = function
    | [] -> acc
    | fn :: fns ->
      if Utils.is_directory fn then
        let more_files =
          try FU.ls fn
          with _ -> [] (* permission denied *)
        in
        loop acc (List.rev_append more_files fns)
      else
        loop (fn :: acc) fns
  in
  List.sort compare (loop [] [dir_name])

let put_one_file rng msg_counter local_node for_DS incoming fn =
  send rng msg_counter local_node for_DS
    (CLI_to_DS (Fetch_file_cmd_req (fn, Local)));
  process_answer incoming do_nothing

let main () =
  (* setup logger *)
  let log_fn = ref "" in
  Log.set_log_level Log.INFO;
  Log.set_output Legacy.stderr;
  Log.color_on ();
  Log.set_prefix (Utils.fg_yellow ^ " CLI" ^ Utils.fg_reset);
  (* options parsing *)
  Arg.parse
    [ "-i", Arg.Set interactive, " interactive mode of the CLI";
      "-c", Arg.Set_string single_command,
      "'command' execute a single command; use quotes if several words";
      "-m", Arg.Set_string machine_file,
      "machine_file list of host:port[:mds_port] (one per line)";
      "-o", Arg.Set_string log_fn, "<filename> where to log";
      "-p", Arg.Set_int cli_port_in, "<port> where the CLI is listening";
      "-v", Arg.Set verbose, " verbose mode"]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !verbose then Log.set_log_level Log.DEBUG;
  if !log_fn <> "" then Log.set_output (Legacy.open_out !log_fn);
  if !machine_file = "" then Utils.abort (Log.fatal "-m is mandatory");
  let hostname = Utils.hostname () in
  let skey, ckey, _int2node, maybe_ds_node, mds_node =
    Utils.data_nodes_array hostname None !machine_file
  in
  Socket_wrapper.setup_keys skey ckey;
  let ds_node = Option.get maybe_ds_node in
  let mds_host = Node.get_host mds_node in
  let mds_port_in = Node.get_port mds_node in
  let ds_host = Node.get_host ds_node in
  let ds_port_in = Node.get_port ds_node in
  let my_rank = Node.get_rank ds_node in (* same rank as our DS node *)
  assert(my_rank <> Utils.default);
  (* local_node does not correspond to a DS so it is initialized dirtily *)
  let local_node = Node.create (-1) hostname !cli_port_in None in
  let ctx = ZMQ.Context.create () in
  let for_MDS = Utils.(zmq_socket Push ctx mds_host mds_port_in) in
  Log.info "Client of MDS %s:%d" mds_host mds_port_in;
  let for_DS = Utils.(zmq_socket Push ctx ds_host ds_port_in) in
  (* continue from a previous session if counter file is found *)
  msg_counter := restore_counter ();
  if !msg_counter = 0 then (* start a new session *)
    begin
      (* register yourself to the local DS by telling it the port you listen to *)
      send rng msg_counter local_node for_DS (CLI_to_DS (Connect_cmd_push !cli_port_in));
      (* register yourself to the MDS *)
      send rng msg_counter local_node for_MDS
        (CLI_to_MDS (Connect_push (my_rank, !cli_port_in)))
    end;
  let incoming = Utils.(zmq_socket Pull ctx "*" !cli_port_in) in
  Log.info "Client of DS %s:%d" ds_host ds_port_in;
  (* the CLI execute just one command then exit *)
  (* we could have a batch mode, executing several commands from a file *)
  let not_finished = ref true in
  try
    while !not_finished do
      not_finished := !interactive;
      let open Command in
      let _before, cmd = read_one_command !interactive in
      match cmd with
      | Skip -> ()
      | Put src_fn ->
        if Utils.is_directory src_fn then
          List.iter
            (put_one_file rng msg_counter local_node for_DS incoming) 
            (ls src_fn)
        else
          put_one_file rng msg_counter local_node for_DS incoming src_fn
      | Get (src_fn, dst_fn) ->
        (* get = extract . fetch *)
        send rng msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        let fetch_cont =
          (fun () -> extract_cmd local_node src_fn dst_fn for_DS incoming)
        in
        process_answer incoming fetch_cont;
        (* let after = Unix.gettimeofday () in *)
        (* Log.info "%.3f" (after -. before) *)
      | Fetch src_fn ->
        send rng msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, Remote)));
        process_answer incoming do_nothing
      | Extract (src_fn, dst_fn) ->
        extract_cmd local_node src_fn dst_fn for_DS incoming
      | Quit ->
        send rng msg_counter local_node for_MDS (CLI_to_MDS Quit_cmd);
        forget_counter ();
        not_finished := false
      | Exit ->
        backup_counter ();
        not_finished := false
      | Ls (detailed, maybe_fn) ->
        send rng msg_counter local_node for_MDS
          (CLI_to_MDS (Ls_cmd_req (my_rank, detailed, maybe_fn)));
        process_answer incoming do_nothing
      | Bcast (src_fn, bcast_method) ->
        send rng msg_counter local_node for_DS
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

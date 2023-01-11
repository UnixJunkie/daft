(* a CLI only talks to the MDS and to the local DS on the same node
   - all complex things should be handled by them and the CLI remain simple *)

open Batteries
open Legacy.Printf
open Types.Protocol

module Fn = Filename
module FU = FileUtil
module Log = Dolog.Log
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
let rng = Utils.create_CSPRNG ()

(* NEEDS_SECURITY_REVIEW *)
let backup_counter fn =
  Utils.with_out_file fn (fun out ->
      (* use a long fixed format to hide the counter range
         from being guessed using the counter file size *)
      fprintf out "%09d\n" !msg_counter
    );
  Log.info "wrote out %s" fn

(* NEEDS_SECURITY_REVIEW *)
let restore_counter fn =
  if Sys.file_exists fn then
    begin
      Log.info "read in %s" fn;
      Utils.with_in_file fn (fun input ->
          int_of_string (Legacy.input_line input)
        )
    end
  else
    0 (* start from fresh *)

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

let get_three: 'a list -> ('a * 'a * 'a) option = function
  | [x; y; z] -> Some (x, y, z)
  | _ -> None

module Command = struct
  type filename = string
  type src_fn = filename
  type dst_fn = filename
  type port = int
  type t = Bcast of src_fn * dst_fn * bcast_method
         | Scat of src_fn * dst_fn
         | Exit (* leave the CLI temporarily *)
         | Extract of src_fn * dst_fn
         | Fetch of filename
         | Get of src_fn * dst_fn
         | Ls of bool * filename option (* ls [-l] [filename] *)
         | Put of src_fn * dst_fn
         | Quit (* turn off whole system *)
         | Skip
  let usage () =
    Log.info
      ("\n" ^^
       "usage: put|get|help|fetch|extract|exit|quit|ls|bcast|scat\n" ^^
       "hotkey ^   ^   ^    ^     ^        ^   ^    ^  ^     ^\n" ^^
       "commands with [optional] parameters:\n" ^^
       "------------------------------------\n" ^^
       "ls [-l] [filename] (list files)\n" ^^
       "bcast src_fn [dst_fn] {chain|bina|bino} \
        ([publish then] broadcast a file using {algorithm})\n" ^^
       "extract src_fn dst_fn (extract file from local datastore to local disk)\n" ^^
       "fetch fn (get a copy in the local datastore of a remote file)\n" ^^
       "get src_fn [dst_fn] (get a local copy of a remote file then extract it)\n" ^^
       "put src_fn [dst_fn] (publish a local file)\n" ^^
       "scat src_fn [dst_fn] (publish a file using scattering strategy)\n")
  (* quick and dirty way to understand a command ASAP *)
  let of_list (do_log: bool) (args: string list): t =
    match args with
    | [] | ["skip"] -> Skip
    | cmd :: args ->
      begin match cmd with
        | "b" | "bcast" ->
          begin match get_two args with
            | Some (fn, bcast_method) ->
              Bcast (fn, fn, Utils.bcast_of_string bcast_method)
            | None ->
              begin match get_three args with
                | Some (src_fn, dst_fn, bcast_method) ->
                  Bcast (src_fn, dst_fn, Utils.bcast_of_string bcast_method)
                | None ->
                  if do_log then
                    Log.error "\nusage: bcast src_fn [dst_fn] {chain|bina|bino}";
                  Skip
              end
          end
        | "e" | "extract" ->
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Extract (src_fn, dst_fn)
            | None -> if do_log then Log.error "\nusage: extract src_fn dst_fn"; Skip
          end
        | "f" | "fetch" ->
          begin match get_one args with
            | Some fn -> Fetch fn
            | None -> if do_log then Log.error "\nusage: fetch fn"; Skip
          end
        | "g" | "get" ->
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Get (src_fn, dst_fn)
            | None ->
              begin match get_one args with
                | Some src_fn -> Get (src_fn, src_fn)
                | None -> if do_log then Log.error "\nusage: get src_fn [dst_fn]"; Skip
              end
          end
        | "h" | "help" -> if do_log then usage(); Skip
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
          begin match get_two args with
            | Some (src_fn, dst_fn) -> Put (Utils.expand_filename src_fn, dst_fn)
            | None ->
              begin match get_one args with
                | Some fn ->
                  let src_fn = Utils.expand_filename fn in
                  Put (src_fn, src_fn)
                | None -> if do_log then Log.error "\nusage: put src_fn [dst_fn]"; Skip
              end
          end
        | "q" | "quit" -> Quit
        | "s" | "scat" ->
          begin match get_one args with
            | Some fn ->
              let src_fn = Utils.expand_filename fn in
              Scat (src_fn, src_fn)
            | None ->
              begin match get_two args with
                | Some (src_fn, dst_fn) -> Scat (Utils.expand_filename src_fn, dst_fn)
                | None ->
                  let () = Log.error "\nusage: scat src_fn [dst_fn]" in
                  Skip
              end
          end
        | "x" | "exit" -> Exit
        | "" ->
          if do_log then begin Log.error "empty command"; usage() end;
          Skip
        | cmd ->
          if do_log then begin Log.error "unknown command: %s" cmd; usage() end;
          Skip
      end
  let is_valid = function
        | "b" | "bcast"
        | "e" | "extract"
        | "f" | "fetch"
        | "g" | "get"
        | "h" | "help"
        | "l" | "ls"
        | "p" | "put"
        | "q" | "quit"
        | "s" | "scat"
        | "x" | "exit" -> true
        | _ -> false
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
        match String.split_on_string ~by:" " command_str with
        | [] -> [""]
        | x -> x
    in
    (before, to_parse)
  in
  (before, Command.of_list true command_line)

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

let put_one_file rng msg_counter local_node for_DS incoming src_fn dst_fn =
  send rng msg_counter local_node for_DS
    (CLI_to_DS (Fetch_file_cmd_req (src_fn, dst_fn, Local)));
  process_answer incoming do_nothing

let extract_daft_command args =
  let l = Array.to_list args in
  let (new_argv: string list), (daft_command: string list) =
    Utils.fold_while Utils.id (fun x -> not (Command.is_valid x)) [] l
  in
  (Array.of_list new_argv, String.concat " " daft_command)

let banner quiet =
  if not quiet then
    printf ("\nWelcome to DAFT! *(^o^)*\n\n" ^^
            " DAFT Allows File Transfers\n" ^^
            " AFTF\n" ^^
            " FTFA\n" ^^
            " TFAD\n\n%!")

let main () =
  let msg_count_fn = "CLI.msg_counter" in
  (* setup logger *)
  let log_fn = ref "" in
  let no_banner = ref false in
  Log.set_log_level Log.INFO;
  Log.set_output Legacy.stderr;
  Log.color_on ();
  Log.set_prefix (Colors.fg_yellow ^ " CLI" ^ Colors.fg_reset);
  (* options parsing *)
  let new_args, daft_command = extract_daft_command Sys.argv in
  single_command := daft_command;
  Arg.parse_argv
    ~current:(ref 0)
    new_args
    [ "-i", Arg.Set interactive, " interactive mode of the CLI";
      "-m", Arg.Set_string machine_file,
      "machine_file list of host:port[:mds_port] (one per line)";
      "-o", Arg.Set_string log_fn, "<filename> where to log";
      "-p", Arg.Set_int cli_port_in, "<port> where the CLI is listening";
      "-q", Arg.Set no_banner, " quiet (no welcome banner)";
      "-v", Arg.Set verbose, " verbose mode"]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0));
  (* check options *)
  if !verbose then Log.set_log_level Log.DEBUG;
  if !log_fn <> "" then Log.set_output (Legacy.open_out !log_fn);
  if !machine_file = "" then Utils.abort (Log.fatal "-m is mandatory");
  banner !no_banner;
  let hostname = Utils.hostname () in
  let lock = sprintf "/tmp/%s:%d.cli.lock" hostname !cli_port_in in
  Utils.acquire_lock lock;
  let skey, ckey, _int2node, maybe_ds_node, mds_node =
    Utils.data_nodes hostname None !machine_file
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
  let ctx = Zmq.Context.create () in
  let for_MDS = Utils.(zmq_socket Push ctx mds_host mds_port_in) in
  Log.info "Client of MDS %s:%d" mds_host mds_port_in;
  let for_DS = Utils.(zmq_socket Push ctx ds_host ds_port_in) in
  (* continue from a previous session if counter file is found *)
  msg_counter := restore_counter msg_count_fn;
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
  let has_quit = ref false in
  try
    while !not_finished do
      not_finished := !interactive;
      let open Command in
      let _before, cmd = read_one_command !interactive in
      match cmd with
      | Skip -> ()
      | Put (src_fn, dst_fn) ->
        if Utils.is_directory src_fn then
          if src_fn <> dst_fn then
            Log.warn "Put src dst: src is a directory so dst is ignored"
          else
            List.iter
              (fun fn -> put_one_file rng msg_counter local_node for_DS incoming fn fn)
              (ls src_fn)
        else
          put_one_file rng msg_counter local_node for_DS incoming src_fn dst_fn
      | Get (src_fn, dst_fn) ->
        (* get = extract . fetch *)
        send rng msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, src_fn, Remote)));
        let fetch_cont =
          (fun () -> extract_cmd local_node src_fn dst_fn for_DS incoming)
        in
        process_answer incoming fetch_cont;
        (* let after = Unix.gettimeofday () in *)
        (* Log.info "%.3f" (after -. before) *)
      | Fetch src_fn ->
        send rng msg_counter local_node for_DS
          (CLI_to_DS (Fetch_file_cmd_req (src_fn, src_fn, Remote)));
        process_answer incoming do_nothing
      | Extract (src_fn, dst_fn) ->
        extract_cmd local_node src_fn dst_fn for_DS incoming
      | Quit ->
        send rng msg_counter local_node for_MDS (CLI_to_MDS Quit_cmd);
        Utils.nuke_file msg_count_fn;
        not_finished := false;
        has_quit := true
      | Exit ->
        backup_counter msg_count_fn;
        not_finished := false
      | Ls (detailed, maybe_fn) ->
        send rng msg_counter local_node for_MDS
          (CLI_to_MDS (Ls_cmd_req (my_rank, detailed, maybe_fn)));
        process_answer incoming do_nothing
      | Bcast (src_fn, dst_fn, bcast_method) ->
        send rng msg_counter local_node for_DS
          (CLI_to_DS (Bcast_file_cmd_req (src_fn, dst_fn, bcast_method)))
      | Scat (src_fn, dst_fn) ->
        let () = send rng msg_counter local_node for_DS
            (CLI_to_DS (Scat_file_cmd_req (src_fn, dst_fn))) in
        process_answer incoming do_nothing
    done;
    raise Types.Loop_end;
  with exn -> begin
      Socket_wrapper.nuke_keys ();
      Utils.nuke_CSPRNG rng;
      Utils.release_lock lock;
      if not !has_quit then backup_counter msg_count_fn;
      Zmq.Socket.close for_MDS;
      Zmq.Socket.close for_DS;
      Zmq.Socket.close incoming;
      Zmq.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end

let () = main ()

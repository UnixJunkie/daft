
open Printf

module FU = FileUtil
module L = List
module Log = Dolog.Log
module Node = Types.Node
module IntMap = Types.IntMap

let default = -1
let default_chunk_size = 1_000_000 (* bytes *)

let fst3 (a, _, _) = a
let snd3 (_, b, _) = b
let trd3 (_, _, c) = c

let string_of_list to_string sep l =
  "[" ^ String.concat sep (L.map to_string l) ^ "]"

let getenv_or_fail variable_name =
  try Sys.getenv variable_name
  with Not_found ->
    Log.error "getenv_or_fail: Sys.getenv: %s" variable_name;
    ""

let sleep_ms ms =
  let (_, _, _) = Unix.select [] [] [] (float_of_int ms /. 1000.) in
  ()

exception Lock_acquired

(* busy wait at 10Hz for ability to create 'dir';
   not very efficient strategy but portable and not introducing a new
   library dependency or reliance on some not always present system
   command. *)
let acquire_lock dir =
  Log.info "acquiring lock";
  try
    while true do
      (* mkdir is atomic on POSIX filesystems *)
      let res = Unix.system ("mkdir " ^ dir ^ " 2>/dev/null") in
      if res = Unix.WEXITED 0 then
        raise Lock_acquired
      else
        sleep_ms 100
    done
  with Lock_acquired -> ()

let release_lock dir =
  Log.info "releasing lock";
  ignore(Unix.system ("rm -rf " ^ dir))

(* like `cmd` in shell
   TODO: use the one in batteries upon next release *)
let run_and_read cmd =
  let string_of_file fn =
    let buff_size = 1024 in
    let buff = Buffer.create buff_size in
    let ic = open_in fn in
    let line_buff = Bytes.create buff_size in
    begin
      let was_read = ref (input ic line_buff 0 buff_size) in
      while !was_read <> 0 do
        Buffer.add_subbytes buff line_buff 0 !was_read;
        was_read := input ic line_buff 0 buff_size;
      done;
      close_in ic;
    end;
    Buffer.contents buff
  in
  let tmp_fn = Filename.temp_file "" "" in
  let cmd_to_run = cmd ^ " > " ^ tmp_fn in
  let status = Unix.system cmd_to_run in
  let output = string_of_file tmp_fn in
  Unix.unlink tmp_fn;
  (status, output)

let is_executable fn =
  Unix.(
    try access fn [X_OK]; true
    with Unix_error (_, _, _) -> false
  )

let is_directory fn =
  FU.test FU.Is_dir fn

let with_in_file fn f =
  let input = open_in fn in
  let res = f input in
  close_in input;
  res

(* same as with_in_file but using a unix file descriptor *)
let with_in_file_descr fn f =
  let input = Unix.(openfile fn [O_RDONLY] 0o400) in
  let res = f input in
  Unix.close input;
  res

let with_out_file fn f =
  let ugo = 0o600 in
  let output = open_out_gen [Open_wronly; Open_creat; Open_trunc] 0o600 fn in
  assert(ugo = 0o600); (* !!! SECURITY: DONT CHANGE UGO RIGHTS  !!! *)
  let res = f output in
  close_out output;
  res

let run_command ?silent:(silent = false) (cmd: string): Unix.process_status =
  if not silent then Log.info "running: %s" cmd;
  Unix.system cmd

(* test if the given command is in $PATH *)
let command_exists (cmd: string): bool =
  Unix.system ("which " ^ cmd ^ " 2>&1 > /dev/null") = Unix.WEXITED 0

let nuke_file fn =
  begin
    (try
       if command_exists "shred" then
         ignore(run_command ~silent:true ("shred " ^ fn ^ " 2>/dev/null"));
     with _ -> ());
    (try Sys.remove fn with _ -> ());
    Log.info "nuked file %s" fn
  end

(* same as with_out_file but using a unix file descriptor *)
let with_out_file_descr fn f =
  let output = Unix.(openfile fn [O_WRONLY; O_CREAT] 0o600) in
  let res = f output in
  Unix.close output;
  res

(* call Unix.read until length bytes were read *)
let really_read
    (input: Unix.file_descr)
    (buff: bytes)
    (length: int): unit =
  assert(length <= Bytes.length buff);
  let was_read = ref 0 in
  while !was_read <> length do
    let to_read = length - !was_read in
    let just_read = Unix.read input buff !was_read to_read in
    was_read := !was_read + just_read
  done

type socket_type = Push | Pull

let zmq_socket
    (t: socket_type) (context: Zmq.Context.t) (host: string) (port: int) =
  let host_and_port = sprintf "tcp://%s:%d" host port in
  match t with
  | Pull ->
    let sock = Zmq.Socket.create context Zmq.Socket.pull in
    Zmq.Socket.bind sock host_and_port;
    sock
  | Push ->
    let sock = Zmq.Socket.create context Zmq.Socket.push in
    Zmq.Socket.connect sock host_and_port;
    (* a push socket must wait forever (upon close) that all its messages
       have been sent *)
    let infinity = -1 in
    Zmq.Socket.set_linger_period sock infinity;
    sock

let ignore_first x y =
  ignore(x);
  y

open Batteries (* everything before uses Legacy IOs (fast) *)

(* returns: (hostname, ds_port, maybe_mds_port) *)
let string_to_host_port (s: string): string * int * int option =
  match BatString.nsplit s ~by:":" with
  | [host; ds_port_str] ->
    (host, int_of_string ds_port_str, None)
  | [host; ds_port_str; mds_port_str] ->
    let mds_port = int_of_string mds_port_str in
    (host, int_of_string ds_port_str, Some mds_port)
  | _ -> ignore_first (Log.fatal "string_to_host_port: %s" s) (exit 1)

let string_list_of_file f =
  List.of_enum (File.lines_of f)

let convert (direction: [< `From_hexa | `To_hexa ]) (message: string): string =
  let hex = match direction with
    | `To_hexa -> Cryptokit.Hexa.encode ()
    | `From_hexa -> Cryptokit.Hexa.decode ()
  in
  hex#put_string message;
  hex#finish;
  hex#get_string

(* generate secret keys to encrypt and sign messages by reading /dev/random *)
(* NEEDS_SECURITY_REVIEW *)
let append_keys (fn: string): unit =
  with_in_file_descr "/dev/urandom" (fun input ->
      let ckey' = Bytes.make 16 '0' in
      let skey' = Bytes.make 20 '0' in
      really_read input ckey' 16;
      really_read input skey' 20;
      let ckey = Bytes.unsafe_to_string ckey' in
      let skey = Bytes.unsafe_to_string skey' in
      assert(String.length ckey = 16);
      assert(String.length skey = 20);
      let skey_hex = convert `To_hexa skey in
      let ckey_hex = convert `To_hexa ckey in
      let prev_contents = string_list_of_file fn in
      (* the machines file is created again with strict perms on purpose *)
      Sys.remove fn;
      with_out_file fn (fun output ->
          Legacy.output_string output (sprintf "skey:%s\n" skey_hex);
          Legacy.output_string output (sprintf "ckey:%s\n" ckey_hex);
          List.iter (fprintf output "%s\n") prev_contents
        );
    )

(* returns (ds_nodes, local_node, mds_node) *)
let parse_machine_file
    (hostname: string) (ds_port: int option) (fn: string)
  : string * string * Node.t list * Node.t option * Node.t =
  (* check file perms since it stores keys *)
  if Unix.((stat fn).st_perm) <> 0o600 then Log.error "perms too wide for %s" fn;
  let port_equal maybe_port p =
    match maybe_port with
    | None -> true
    | Some q -> (q = p)
  in
  let dummy_node = Node.dummy () in
  let mds_node = ref dummy_node in
  let local_ds_node = ref None in
  let parse_machine_line (rank: int) (l: string): Node.t =
    let host, port, maybe_mds_port = string_to_host_port l in
    begin match maybe_mds_port with
      | None -> ()
      | Some mds_port ->
        if !mds_node <> dummy_node then
          failwith ("parse_machine_file: too many 'host:ds_port:mds_port' \
                     lines in " ^ fn)
        else
          mds_node := Node.create (-1) host mds_port None
    end;
    let res = Node.create rank host port None in
    if host = hostname && port_equal ds_port port
    then local_ds_node := Some res;
    res
  in
  (* read all lines *)
  let lines = List.map String.strip (string_list_of_file fn) in
  (* remove key lines *)
  let skey_lines, other_lines =
    List.partition (fun s -> String.starts_with s "skey:") lines
  in
  let ckey_lines, machine_lines =
    List.partition (fun s -> String.starts_with s "ckey:") other_lines
  in
  let nb_skey_lines = List.length skey_lines in
  let nb_ckey_lines = List.length ckey_lines in
  if nb_skey_lines = 0 then failwith ("not skey: line in " ^ fn);
  if nb_ckey_lines = 0 then failwith ("not ckey: line in " ^ fn);
  if nb_skey_lines > 1 then failwith ("too many skey: lines in " ^ fn);
  if nb_ckey_lines > 1 then failwith ("too many ckey: lines in " ^ fn);
  let skey_hex = String.lchop ~n:(String.length "skey:") (List.hd skey_lines) in
  let ckey_hex = String.lchop ~n:(String.length "ckey:") (List.hd ckey_lines) in
  let skey = convert `From_hexa skey_hex in
  let ckey = convert `From_hexa ckey_hex in
  let res = List.mapi parse_machine_line machine_lines in
  if !mds_node = dummy_node then
    failwith ("missing 'host:ds_port:mds_port' line in " ^ fn)
  else
    (skey, ckey, res, !local_ds_node, !mds_node)

exception Found of int

let get_ds_rank (host: string) (port: int) (nodes: Node.t list): int =
  try
    List.iter (fun n ->
        if Node.get_host n = host && Node.get_port n = port then
          raise (Found (Node.get_rank n))
      ) nodes;
    failwith (sprintf "get_ds_rank: no such ds: %s:%d" host port)
  with Found i -> i

let data_nodes (hostname: string) (ds_port: int option) (fn: string) =
  let skey, ckey, machines, local_ds_node, mds_node =
    parse_machine_file hostname ds_port fn
  in
  let res =
    L.fold_left (fun acc node ->
        IntMap.add (Node.get_rank node) (node, None, None) acc
      ) IntMap.empty machines
  in
  (skey, ckey, ref res, local_ds_node, mds_node)

let cleanup_data_nodes warn a =
  IntMap.iter (fun i (_ds, maybe_ds_sock, maybe_cli_sock) ->
      match maybe_ds_sock, maybe_cli_sock with
      | Some ds_sock, Some cli_sock ->
        Zmq.Socket.close ds_sock;
        Zmq.Socket.close cli_sock
      | Some ds_sock, None ->
        Zmq.Socket.close ds_sock
      | None, None -> if warn then Log.warn "DS %d missing" i
      | None, Some cli_sock ->
        if warn then Log.warn "DS %d missing" i;
        Zmq.Socket.close cli_sock
    ) a

let count_char (c: char) (s: string): int =
  let res = ref 0 in
  String.iter (fun c' -> if c = c' then incr res) s;
  !res

let hostname (): string =
  let open Unix in
  let host_entry = gethostbyname (gethostname ()) in
  let n1 = host_entry.h_name in
  let l1 = String.length n1 in
  let res =
    if Array.length host_entry.h_aliases = 0 then
      n1
    else
      let n2 = host_entry.h_aliases.(0) in
      let l2 = String.length n2 in
      if l1 > l2 then
        n1
      else
        ignore_first
          (Log.warn "hostname: host alias (%s) longer than FQDN (%s)" n2 n1)
          n2
  in
  if count_char '.' res < 2 then Log.warn "hostname: FQ hostname: %s" res;
  res

let bcast_of_string = function
  | "chain" -> Types.Protocol.Chain
  | "bina" -> Types.Protocol.Binary
  | "bino" -> Types.Protocol.Binomial
  | x ->
    let default = Types.Protocol.Chain in
    Log.warn "bcast_of_string: unsupported: %s (supported: chain|bina|bino)" x;
    Log.warn "bcast_of_string: falling back to chain";
    default

let string_of_bcast = function
  | Types.Protocol.Chain -> "chain"
  | Types.Protocol.Binary -> "bina"
  | Types.Protocol.Binomial -> "bino"

exception Fatal (* throw this when we are doomed *)

let abort _log =
  raise Fatal

(* NEEDS_SECURITY_REVIEW *)
let create_CSPRNG (): Cryptokit.Random.rng =
  Cryptokit.(Random.pseudo_rng (Random.string Random.secure_rng 16))

(* NEEDS_SECURITY_REVIEW *)
let nuke_CSPRNG (rng: Cryptokit.Random.rng): unit =
  rng#wipe

(* convert a relative path to an absolute path, also handles filenames
   starting with ~/ and ./ *)
let expand_filename (fn: string): string =
  if String.starts_with fn "/" then
    fn (* already absolute *)
  else if String.starts_with fn "~/" then
    (* relative to $HOME to absolute conversion *)
    let path = String.lchop ~n:2 fn in
    let home = getenv_or_fail "HOME" in
    home ^ "/" ^ path
  else
    (* relative to absolute conversion *)
    let cwd = Unix.getcwd () in
    if String.starts_with fn "./" then
      cwd ^ "/" ^ (String.lchop ~n:2 fn)
    else
      cwd ^ "/" ^ fn

(* fold_left f on l while p is true *)
let rec fold_while f p acc = function
  | [] -> (List.rev acc, [])
  | x :: xs ->
    if p x then
      fold_while f p (f x :: acc) xs
    else
      (List.rev acc, x :: xs)

let id x =
  x

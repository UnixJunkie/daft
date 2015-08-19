
open Printf

module A = Array
module L = List
module Node = Types.Node

let default_bcast_algo = `Amoeba

let uninitialized = -1
let default_mds_host = "*" (* star means all interfaces *)
let default_ds_port_in  = 8081
let default_mds_port_in = 8082
let default_chunk_size = 1_000_000 (* bytes *)

(* ANSI terminal colors for UNIX: *)
let fg_black = "\027[30m"
let fg_red = "\027[31m"
let fg_green = "\027[32m"
let fg_yellow = "\027[33m"
let fg_blue = "\027[34m"
let fg_magenta = "\027[35m"
let fg_cyan = "\027[36m"
let fg_white = "\027[37m"
let fg_default = "\027[39m"
let fg_reset = "\027[0m"

let fst3 (a, _, _) = a
let snd3 (_, b, _) = b
let trd3 (_, _, c) = c

let getenv_or_fail variable_name =
  try Sys.getenv variable_name
  with Not_found ->
    Log.error "getenv_or_fail: Sys.getenv: %s" variable_name;
    ""

let sleep_ms ms =
  let (_, _, _) = Unix.select [] [] [] (float_of_int ms /. 1000.) in
  ()

let out_of_bounds i a =
  i < 0 || i > (A.length a) - 1

(* check existence of a file *)
let file_or_link_exists fn =
  try
    let _ = Unix.lstat fn in
    true
  with _ -> false

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
        Buffer.add_substring buff line_buff 0 !was_read;
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
  let open Unix in
  try
    access fn [X_OK];
    true
  with Unix_error (_, _, _) ->
    false

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
  let output = open_out fn in
  let res = f output in
  close_out output;
  res

(* same as with_out_file but using a unix file descriptor *)
let with_out_file_descr fn f =
  let output = Unix.(openfile fn [O_WRONLY; O_CREAT] 0o600) in
  let res = f output in
  Unix.close output;
  res

(* call Unix.read until length bytes were read *)
let really_read
    (input: Unix.file_descr)
    (buff: string)
    (length: int): unit =
  assert(length <= String.length buff);
  let was_read = ref 0 in
  while !was_read <> length do
    let to_read = length - !was_read in
    let just_read = Unix.read input buff !was_read to_read in
    was_read := !was_read + just_read
  done

type socket_type = Push | Pull

let zmq_socket (t: socket_type) (context: ZMQ.Context.t) (host: string) (port: int) =
  let host_and_port = sprintf "tcp://%s:%d" host port in
  match t with
  | Pull ->
    let sock = ZMQ.Socket.create context ZMQ.Socket.pull in
    ZMQ.Socket.bind sock host_and_port;
    sock
  | Push ->
    let sock = ZMQ.Socket.create context ZMQ.Socket.push in
    ZMQ.Socket.connect sock host_and_port;
    (* a push socket must wait forever (upon close) that all its messages
       have been sent *)
    let infinity = -1 in
    ZMQ.Socket.set_linger_period sock infinity;
    sock

open Batteries (* everything before uses Legacy IOs (fast) *)

let string_to_host_port (s: string): string * int =
  let host, port_str = BatString.split ~by:":" s in
  (host, int_of_string port_str)

let set_host_port (host_ref: string ref) (port_ref: int ref) (s: string) =
  let host, port = string_to_host_port s in
  host_ref := host;
  port_ref := port

let parse_machine_file (fn: string): Node.t list =
  let parse_machine_line (rank: int) (l: string): Node.t =
    let host, port = string_to_host_port l in
    Node.create rank host port None
  in
  let res = ref [] in
  with_in_file fn
    (fun input ->
       try
         let i = ref 0 in
         while true do
           res := (parse_machine_line !i (Legacy.input_line input)) :: !res;
           incr i;
         done
       with End_of_file -> ()
    );
  L.rev !res

exception Found of int

let get_ds_rank (host: string) (port: int) (nodes: Node.t list): int =
  try
    List.iter (fun n ->
        if Node.get_host n = host && Node.get_ds_port n = port then
          raise (Found (Node.get_rank n))
      ) nodes;
    failwith (sprintf "get_ds_rank: no such ds: %s:%d" host port)
  with Found i -> i

let data_nodes_array (fn: string) =
  let machines = parse_machine_file fn in
  let len = L.length machines in
  let res = A.make len (Node.dummy (), None, None) in
  L.iter (fun node -> A.set res (Node.get_rank node) (node, None, None)
         ) machines;
  res

let cleanup_data_nodes_array warn a =
  A.iteri (fun i (_ds, maybe_ds_sock, maybe_cli_sock) ->
      match maybe_ds_sock, maybe_cli_sock with
      | Some ds_sock, Some cli_sock ->
        ZMQ.Socket.close ds_sock;
        ZMQ.Socket.close cli_sock
      | Some ds_sock, None ->
        ZMQ.Socket.close ds_sock
      | None, None -> if warn then Log.warn "DS %d missing" i
      | None, Some cli_sock ->
        if warn then Log.warn "DS %d missing" i;
        ZMQ.Socket.close cli_sock
    ) a

let ignore_first x y =
  ignore(x);
  y

let count_char (c: char) (s: string): int =
  let res = ref 0 in
  String.iter (fun c' -> if c = c' then incr res) s;
  !res

let hostname (): string =
  let open Unix in
  let host_entry = gethostbyname (gethostname ()) in
  let n1 = host_entry.h_name in
  let l1 = String.length n1 in
  let n2 = host_entry.h_aliases.(0) in
  let l2 = String.length n2 in
  let res =
    if l1 > l2 then n1
    else
      ignore_first
        (Log.warn "hostname: host alias (%s) longer than FQDN (%s)" n2 n1)
        n2
  in
  if count_char '.' res <> 2 then Log.warn "hostname: FQ hostname: %s" res;
  res

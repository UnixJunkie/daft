
open Printf

module A = Array
module L = List
module Node = Types.Node

let uninitialized = -1
let default_mds_host = "*" (* star means all interfaces *)
let default_ds_port_in  = 8081
let default_mds_port_in = 8082
let default_cli_port_in = 8000
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

let sleep_ms ms =
  let (_, _, _) = Unix.select [] [] [] (float_of_int ms /. 1000.) in
  ()

(* like `cmd` in shell
   TODO: use the one in batteries upon next release *)
let run_and_read cmd =
  let string_of_file fn =
    let buff_size = 1024 in
    let buff = Buffer.create buff_size in
    let ic = open_in fn in
    let line_buff = String.create buff_size in
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

let with_in_file fn f =
  let input = open_in fn in
  let res = f input in
  close_in input;
  res

let with_out_file fn f =
  let output = open_out fn in
  let res = f output in
  close_out output;
  res

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

let hostname (): string =
  let stat, res = run_and_read "hostname -f" in
  assert(stat = Unix.WEXITED 0);
  String.strip res (* rm trailing \n *)

let string_to_host_port (s: string): string * int =
  let host, port_str = BatString.split ~by:":" s in
  (host, int_of_string port_str)

let set_host_port (host_ref: string ref) (port_ref: int ref) (s: string) =
  let host, port = string_to_host_port s in
  host_ref := host;
  port_ref := port

let parse_machine_line (rank: int) (l: string): Node.t =
  let host, port = string_to_host_port l in
  Node.create rank host port

let parse_machine_file (fn: string): Node.t list =
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

let data_nodes_array (fn: string) =
  let machines = parse_machine_file fn in
  let len = L.length machines in
  let res = A.create len (Node.dummy (), None) in
  L.iter (fun node -> A.set res (Node.get_rank node) (node, None)
         ) machines;
  res

let cleanup_data_nodes_array warn a =
  A.iteri (fun i (_ds, maybe_sock) -> match maybe_sock with
      | Some s -> ZMQ.Socket.close s
      | None -> if warn then Log.warn "DS %d missing" i
    ) a

open Batteries
open Printf
open Types.Protocol

let ds_in_yellow = Utils.fg_yellow ^ "DS" ^ Utils.fg_reset

module A  = Array
module Fn = Filename
module From_DS = Types.Protocol.From_DS
module From_MDS = Types.Protocol.From_MDS
module FU = FileUtil
module Logger = Log
(* prefix all logs *)
module Log = Log.Make (struct let section = ds_in_yellow end)
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet
module Sock = ZMQ.Socket

let uninitialized = -1

(* setup data server *)
let ds_log_fn = ref ""
let ds_host = Utils.hostname ()
let ds_port_in = ref Utils.default_ds_port_in
let cli_port_in = ref Utils.default_cli_port_in
let ds_rank = ref uninitialized
let machine_file = ref ""
let mds_host = ref "localhost"
let mds_port_in = ref Utils.default_mds_port_in
let chunk_size = ref Utils.default_chunk_size (* DAFT global constant *)
let local_state = ref FileSet.empty
let data_store_root = ref ""
let local_node = ref (Node.dummy ()) (* this node *)

let abort msg =
  Log.fatal msg;
  exit 1

(* create local data store with unix UGO rights 700
   we take into account the port so that several DSs can be started on one
   host at the same time, for tests *)
let create_data_store (): string =
  assert(!local_node <> Node.dummy ());
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root =
    sprintf "%s/%s.ds" tmp_dir (Node.to_string !local_node)
  in
  Unix.mkdir data_store_root 0o700;
  Log.info "I store in %s" data_store_root;
  data_store_root

(* destroy a data store *)
let delete_data_store (ds: string): int =
  Sys.command ("rm -rf " ^ ds)

(* how many there are and size of the last one if < chunk_size *)
let compute_chunks (size: int64) =
  let ratio =
    (Int64.to_float size) /. (float_of_int File.Chunk.default_size)
  in
  (* total number of chunks *)
  let nb_chunks = int_of_float (ceil ratio) in
  (* number of full chunks *)
  let nb_chunks_i64 = Int64.of_int (int_of_float ratio) in
  let chunk_size_i64 = Int64.of_int File.Chunk.default_size in
  let last_chunk_size_i64 = Int64.(size - (nb_chunks_i64 * chunk_size_i64)) in
  let last_chunk_size_opt = if last_chunk_size_i64 <> Int64.zero
                            then Some last_chunk_size_i64
                            else None
  in
  (nb_chunks, last_chunk_size_opt)

let add_file (fn: string): ds_to_cli =
  if FileSet.contains_fn fn !local_state then
    Add_file_cmd_nack (fn, Already_here)
  else if Sys.is_directory fn then
    Add_file_cmd_nack (fn, Is_directory)
  else
    FU.( (* opened FU to get rid of warning 40 *)
      let stat = FU.stat fn in
      let size = FU.byte_of_size stat.size in
      let fn =
        if S.starts_with fn "/" (* chop leading '/' if any *)
        then S.lchop ~n:1 fn
        else fn
      in
      let dest_fn = !data_store_root ^ "/" ^ fn in
      let dest_dir = Fn.dirname dest_fn in
      (* mkdir create all necessary parent dirs *)
      FU.mkdir ~parent:true ~mode:0o700 dest_dir;
      FU.cp ~follow:FU.Follow ~force:FU.Force ~recurse:false [fn] dest_fn;
      (* check cp succeeded based on new file's size *)
      let stat' = FU.stat dest_fn in
      if stat'.size <> stat.size then
        Add_file_cmd_nack (fn, Copy_failed)
      else begin (* update local state *)
        let nb_chunks, last_chunk_size = compute_chunks size in
        (* n.b. we keep the stat struct from the original file *)
        let new_file =
          File.create fn size stat nb_chunks last_chunk_size !local_node
        in
        local_state := FileSet.add new_file !local_state;
        Add_file_cmd_ack fn
      end
    )

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-cs", Arg.Set_int chunk_size, "<size> file chunk size";
      "-l", Arg.Set_string ds_log_fn, "<filename> where to log";
      "-cli", Arg.Set_int cli_port_in, "<port> where the CLI is listening";
      "-p", Arg.Set_int ds_port_in, "<port> where to listen";
      "-r", Arg.Set_int ds_rank, "<rank> rank among other data nodes";
      "-m", Arg.Set_string machine_file,
      "machine_file list of [user@]host:port (one per line)";
      "-mds", Arg.Set_string mds_host, "<server> MDS host";
      "-mdsp", Arg.Set_int mds_port_in, "<port> MDS port" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !ds_log_fn <> "" then begin (* don't log anything before that *)
    let log_out = Legacy.open_out !ds_log_fn in
    Logger.set_output log_out
  end;
  if !chunk_size = uninitialized then (Log.fatal "-cs is mandatory"; exit 1);
  if !mds_host = "" then (Log.fatal "-mds is mandatory"; exit 1);
  if !mds_port_in = uninitialized then (Log.fatal "-sp is mandatory"; exit 1);
  if !ds_rank = uninitialized then (Log.fatal "-r is mandatory"; exit 1);
  if !ds_port_in = uninitialized then (Log.fatal "-p is mandatory"; exit 1);
  if !cli_port_in = uninitialized then (Log.fatal "-cli is mandatory"; exit 1);
  if !machine_file = "" then abort "-m is mandatory";
  let int2node = Utils.data_nodes_array !machine_file in
  Log.info "read %d host(s)" (A.length int2node);
  local_node := Node.create !ds_rank ds_host !ds_port_in;
  Log.info "Client of MDS %s:%d" !mds_host !mds_port_in;
  data_store_root := create_data_store ();
  (* setup server *)
  Log.info "binding server to %s:%d" "*" !ds_port_in;
  let ctx = ZMQ.Context.create () in
  let incoming = Utils.(zmq_socket Pull ctx "*" !ds_port_in) in
  (* feedback socket to the local CLI *)
  let to_cli = Utils.(zmq_socket Push ctx ds_host !cli_port_in) in
  (* register at the MDS *)
  Log.info "connecting to MDS %s:%d" !mds_host !mds_port_in;
  let to_mds = Utils.(zmq_socket Push ctx !mds_host !mds_port_in) in
  let here_I_am = From_DS.encode (From_DS.To_MDS (Join_push !local_node)) in
  Sock.send to_mds here_I_am;
  (* FBR: create an array of DS sockets for sending *)
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      let encoded = Sock.recv incoming in
      let message = For_DS.decode encoded in
      begin match message with
        | For_DS.From_MDS (Send_to_req (_ds_rank, _fn, _chunk)) -> (* ------ *)
          abort "Send_to_req"
        | For_DS.From_MDS Quit_cmd -> (* ----------------------------------- *)
          let _ = Log.info "got Quit" in
          not_finished := false
        | For_DS.From_MDS (Add_file_ack fn) -> (* -------------------------- *)
          let ack = For_CLI.encode (For_CLI.From_DS (Add_file_cmd_ack fn)) in
          Sock.send to_cli ack
        | For_DS.From_MDS (Add_file_nack fn) -> (* ------------------------- *)
          let nack =
            For_CLI.encode (For_CLI.From_DS (Add_file_cmd_nack (fn, Already_here)))
          in
          Sock.send to_cli nack
        | For_DS.From_DS (Chunk (_fn, _chunk, _data)) -> (* ---------------- *)
          abort "Chunk"
        | For_DS.From_CLI Add_file_cmd_req _f -> (* ------------------------ *)
          abort "Add_file_cmd_req"
      end
    done;
  with exn -> begin
      Log.error "exception";
      let (_: int) = delete_data_store !data_store_root in
      ZMQ.Socket.close incoming;
      ZMQ.Socket.close to_mds;
      ZMQ.Context.terminate ctx;
      raise exn
    end
;;

main ()

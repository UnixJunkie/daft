open Batteries
open Printf
open Types.Protocol

let ds_in_yellow = Utils.fg_yellow ^ "DS" ^ Utils.fg_reset

module A  = Array
module Fn = Filename
module FU = FileUtil
module Logger = Log
(* prefix all logs *)
module Log = Log.Make (struct let section = ds_in_yellow end)
module S = String
module Node = Types.Node
module File = Types.File
module FileSet = Types.FileSet
module Chunk = File.Chunk
module ChunkSet = File.ChunkSet
module Sock = ZMQ.Socket

(* setup data server *)
let ds_log_fn = ref ""
let ds_host = Utils.hostname ()
let ds_port_in = ref Utils.default_ds_port_in
let cli_host = ref ""
let cli_port_in = ref Utils.default_cli_port_in
let ds_rank = ref Utils.uninitialized
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
  Unix.mkdir data_store_root 0o700; (* access rights for owner only *)
  Log.info "I store in %s" data_store_root;
  data_store_root

(* destroy a data store *)
let delete_data_store (ds: string): int =
  Sys.command ("rm -rf " ^ ds)

(* how many there are and size of the last one if < chunk_size *)
let compute_chunks (size: int64) =
  let ratio = (Int64.to_float size) /. (float_of_int !chunk_size) in
  (* total number of chunks *)
  let nb_chunks = int_of_float (ceil ratio) in
  (* number of full chunks *)
  let nb_chunks_i64 = Int64.of_int (int_of_float ratio) in
  let chunk_size_i64 = Int64.of_int !chunk_size in
  let last_chunk_size_i64 = Int64.(size - (nb_chunks_i64 * chunk_size_i64)) in
  let last_chunk_size_opt = if last_chunk_size_i64 <> Int64.zero
                            then Some last_chunk_size_i64
                            else None
  in
  (nb_chunks, last_chunk_size_opt)

(* get the path of file fn in the local datastore *)
let fn_to_path (fn: Types.filename): string =
  !data_store_root ^
  if S.starts_with fn "/" then fn
  else "/" ^ fn

let add_file (fn: string): ds_to_cli =
  if FileSet.contains_fn fn !local_state then
    Fetch_file_cmd_nack (fn, Already_here)
  else if Sys.is_directory fn then
    Fetch_file_cmd_nack (fn, Is_directory)
  else
    FU.(
      let stat = FU.stat fn in
      let size = FU.byte_of_size stat.size in
      let dest_fn = fn_to_path fn in
      let dest_dir = Fn.dirname dest_fn in
      (* mkdir creates all necessary parent dirs *)
      FU.mkdir ~parent:true ~mode:0o700 dest_dir;
      FU.cp ~follow:FU.Follow ~force:FU.Force ~recurse:false [fn] dest_fn;
      (* keep only read (and optionally exec) perms for the user *)
      if Utils.is_executable fn
      then Unix.chmod dest_fn 0o500
      else Unix.chmod dest_fn 0o400;
      (* check cp succeeded based on new file's size *)
      let stat' = FU.stat dest_fn in
      if stat'.size <> stat.size then
        Fetch_file_cmd_nack (fn, Copy_failed)
      else begin (* update local state *)
        let nb_chunks, last_chunk_size = compute_chunks size in
        let all_chunks = File.all_chunks nb_chunks last_chunk_size !local_node in
        let new_file = File.create fn size nb_chunks all_chunks in
        local_state := FileSet.add new_file !local_state;
        Fetch_file_cmd_ack fn
      end)

exception Invalid_ds_rank

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-cs", Arg.Set_int chunk_size, "<size> file chunk size";
      "-l", Arg.Set_string ds_log_fn, "<filename> where to log";
      "-cli", Arg.String (Utils.set_host_port cli_host cli_port_in),
      "<host:port> CLI";
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
  if !chunk_size = Utils.uninitialized then abort "-cs is mandatory";
  if !mds_host = "" then abort "-mds is mandatory";
  if !mds_port_in = Utils.uninitialized then abort "-sp is mandatory";
  if !ds_rank = Utils.uninitialized then abort "-r is mandatory";
  if !ds_port_in = Utils.uninitialized then abort "-p is mandatory";
  if !cli_host = "" then abort "-cli is mandatory";
  if !cli_port_in = Utils.uninitialized then abort "-cli is mandatory";
  if !machine_file = "" then abort "-m is mandatory";
  let ctx = ZMQ.Context.create () in
  let int2node = Utils.data_nodes_array !machine_file in
  (* create a push socket for each DS, except the current one because we
     will never send to self *)
  A.iteri (fun i (node, _sock) ->
      if i <> !ds_rank then
        let sock =
          Utils.(zmq_socket Push ctx (Node.get_host node) (Node.get_port node))
        in
        A.set int2node i (node, Some sock)
    ) int2node;
  Log.info "read %d host(s)" (A.length int2node);
  local_node := Node.create !ds_rank ds_host !ds_port_in;
  Log.info "Client of MDS %s:%d" !mds_host !mds_port_in;
  data_store_root := create_data_store ();
  (* setup server *)
  Log.info "binding server to %s:%d" "*" !ds_port_in;
  let incoming = Utils.(zmq_socket Pull ctx "*" !ds_port_in) in
  (* feedback socket to the local CLI *)
  let to_cli = Utils.(zmq_socket Push ctx !cli_host !cli_port_in) in
  (* register at the MDS *)
  Log.info "connecting to MDS %s:%d" !mds_host !mds_port_in;
  let to_mds = Utils.(zmq_socket Push ctx !mds_host !mds_port_in) in
  let here_I_am = encode (DS_to_MDS (Join_push !local_node)) in
  Sock.send to_mds here_I_am;
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      let encoded = Sock.recv incoming in
      let message = decode encoded in
      begin match message with
        | MDS_to_DS (Send_to_req (ds_rank, fn, chunk_id, is_last)) ->
          begin
            Log.debug "got Send_to_req";
            try (* 1) check we have this file *)
              (* enforce ds_rank bounds because array access soon *)
              if Utils.out_of_bounds ds_rank int2node
              then raise Invalid_ds_rank;
              let file = FileSet.find_fn fn !local_state in
              let chunks = File.get_chunks file in
              try (* 2) check we have this chunk *)
                let chunk = ChunkSet.find_id chunk_id chunks in
                (* 3) seek to it *)
                let local_file = fn_to_path fn in
                let chunk_data =
                  Utils.with_in_file_descr local_file (fun input ->
                      let offset = chunk_id * !chunk_size in
                      assert(Unix.(lseek input offset SEEK_SET) = offset);
                      let curr_chunk_size = match Chunk.get_size chunk with
                        | None -> !chunk_size
                        | Some s -> Int64.to_int s
                      in
                      (* WARNING: data copy here and allocation of 
                                  a fresh buffer each time *)
                      let buff = String.create curr_chunk_size in
                      assert(Unix.read input buff 0 curr_chunk_size =
                             curr_chunk_size);
                      buff
                    )
                in
                (* 4) send it *)
                let to_send =
                  encode
                    (DS_to_DS (Chunk (fn, chunk_id, is_last, chunk_data)))
                in
                begin match int2node.(ds_rank) with
                  | (_node, Some to_node_i) -> Sock.send to_node_i to_send
                  | (_, None) -> assert(false)
                end
              with Not_found ->
                Log.error "no such chunk: fn: %s id: %d" fn chunk_id
            with
            | Not_found ->
              Log.error "Send_to_req: no such file: %s" fn
            | Invalid_ds_rank ->
              Log.error "Send_to_req: invalid rank: %d" ds_rank
          end
        | MDS_to_DS Quit_cmd ->
          Log.debug "got Quit_cmd";
          let _ = Log.info "got Quit" in
          not_finished := false
        | MDS_to_DS (Add_file_ack fn) ->
          Log.debug "got Add_file_ack";
          (* forward the good news to the CLI *)
          let ack = encode (DS_to_CLI (Fetch_file_cmd_ack fn)) in
          Sock.send to_cli ack
        | MDS_to_DS (Add_file_nack fn) ->
          Log.debug "got Add_file_nack";
          Log.warn "datastore: no rollback for %s" fn;
          (* quick and dirty but maybe OK: only rollback local state's view *)
          local_state := FileSet.remove_fn fn !local_state;
          let nack = (* forward nack to CLI *)
            encode (DS_to_CLI (Fetch_file_cmd_nack (fn, Already_here)))
          in
          Sock.send to_cli nack
        | DS_to_DS (Chunk (fn, chunk_id, is_last, data)) ->
          begin
            Log.debug "got Chunk";
            let size = String.length data in
            let size64 = Int64.of_int size in
            let curr_chunk_size =
              if size <> !chunk_size then Some size64 else None
            in
            if not is_last && Option.is_some curr_chunk_size then
              Log.error "invalid chunk size: fn: %s id: %d size: %d"
                fn chunk_id size
            else begin
              (* 1) write chunk at offset *)
              let offset = chunk_id * !chunk_size in
              let local_file = fn_to_path fn in
              Utils.with_out_file_descr local_file (fun out ->
                  assert(Unix.(lseek out offset SEEK_SET) = offset);
                  assert(Unix.write_substring out data 0 size = size)
                );
              (* 2) update local state *)
              let file =
                try FileSet.find_fn fn !local_state (* retrieve it *)
                with Not_found -> (* or create it *)
                  let unknown_size = Int64.of_int (-1) in
                  let unknown_total_chunks = -1 in
                  File.create
                    fn unknown_size unknown_total_chunks ChunkSet.empty
              in
              let prev_chunks = File.get_chunks file in
              let curr_chunk =
                File.Chunk.create chunk_id curr_chunk_size !local_node
              in
              (* only once we receive the last chunk can we finalize the file's
                 description in local_state *)
              let new_file =
                if is_last then (* finalize file description *)
                  let full_size = Int64.(of_int offset + size64) in
                  let total_chunks = chunk_id + 1 in
                  let curr_chunks = ChunkSet.add curr_chunk prev_chunks in
                  File.create fn full_size total_chunks curr_chunks
                else (* just update chunkset *)
                  File.add_chunk file curr_chunk
              in
              local_state := FileSet.update_fn fn new_file !local_state;
              (* 3) notify MDS *)
              let got_chunk = encode (DS_to_MDS (Chunk_ack (fn, chunk_id))) in
              Sock.send to_mds got_chunk;
              (* 4) notify CLI if all file chunks have been received *)
              let curr_chunks = File.get_chunks new_file in
              let nb_chunks = File.get_nb_chunks new_file in
              if ChunkSet.cardinal curr_chunks = nb_chunks then (
                let got_file = encode (DS_to_CLI (Fetch_file_cmd_ack fn)) in
                Sock.send to_cli got_file
              );
            end
          end
        | CLI_to_DS (Fetch_file_cmd_req (fn, Local)) ->
          Log.debug "got Fetch_file_cmd_req:Local";
          let res = add_file fn in
          begin match res with
            | Fetch_file_cmd_ack fn ->
              (* notify MDS about this new file *)
              let file = FileSet.find_fn fn !local_state in
              let add_file_req = Add_file_req (!ds_rank, file) in
              let add_file_msg = encode (DS_to_MDS add_file_req) in
              Sock.send to_mds add_file_msg
            | Fetch_file_cmd_nack (fn, err) ->
              let nack = encode (DS_to_CLI (Fetch_file_cmd_nack (fn, err))) in
              Sock.send to_cli nack
          end
        | CLI_to_DS (Fetch_file_cmd_req (fn, Remote)) ->
          Log.debug "got Fetch_file_cmd_req:Remote";
          (* finish quickly in case file is already present locally *)
          if FileSet.contains_fn fn !local_state then
            let ack = encode (DS_to_CLI (Fetch_file_cmd_ack fn)) in
            Log.info "%s already here" fn;
            Sock.send to_cli ack
          else (* forward request to MDS *)
            let req = encode
                (CLI_to_MDS (Fetch_cmd_req (Node.get_rank !local_node, fn)))
            in
            Sock.send to_mds req
        | DS_to_CLI  _ -> Log.warn "DS_to_CLI"
        | MDS_to_CLI _ -> Log.warn "MDS_to_CLI"
        | DS_to_MDS  _ -> Log.warn "DS_to_MDS"
        | CLI_to_MDS _ -> Log.warn "CLI_to_MDS"
      end
    done;
    raise Types.Loop_end;
  with exn -> begin
      let (_: int) = delete_data_store !data_store_root in
      ZMQ.Socket.close incoming;
      ZMQ.Socket.close to_mds;
      ZMQ.Socket.close to_cli;
      let dont_warn = false in
      Utils.cleanup_data_nodes_array dont_warn int2node;
      ZMQ.Context.terminate ctx;
      begin match exn with
        | Types.Loop_end -> ()
        | _ -> raise exn
      end
    end
;;

main ()

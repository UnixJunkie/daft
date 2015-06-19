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
module Socket = Socket_wrapper.DS_socket

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
      Utils.ignore_first
        (Log.warn "hostname: host alias (%s) longer than FQDN (%s)" n2 n1)
        n2
  in
  if count_char '.' res <> 2 then Log.warn "hostname: FQ hostname: %s" res;
  res

module Amoeba = struct
  (* broadcasting technique:
     compute two children per node and stop once we are looping *)
  let incr_mod i nb_nodes =
    (i + 1) mod nb_nodes
  (* when you receive a chunk to broadcast: you call fork to know
     if and to who you need to forward those chunks *)
  let fork ((root_rank, step_num, my_rank, nb_nodes) as params) =
    let i = (my_rank + step_num + 1) mod nb_nodes in
    let j = incr_mod i nb_nodes in
    if i = root_rank then ([], params)
    else if j = root_rank then ([i], params)
    else ([i; j], (root_rank, step_num + 1, incr_mod my_rank nb_nodes, nb_nodes))
  (* ranks is for tests in the toplevel only *)
  let rec ranks root_rank step_num my_rank nb_nodes acc =
    match fork (root_rank, step_num, my_rank, nb_nodes) with
    | [], _ -> List.rev acc
    | [i], _ -> List.rev ((my_rank, [i]) :: acc)
    | [i; j], (rr, sn, mr, nb) -> ranks rr sn mr nb ((my_rank, [i; j]) :: acc)
    | _ -> assert(false)
end

let compute_children_bino (my_rank_i: int) (max_rank_i: int): int list =
  (* for CCO: there is a bug: 9 appears two times as a dest
     0 1
     0 2
     0 4
     0 8
     1 3
     1 5
     1 9
     2 6
     3 9 *)
  let logbin a =
    if a = 0.0 then -1.0
    else log a /. log 2.0
  in
  (* Who are my children? *)
  let ds_rank = float_of_int my_rank_i in
  let max_rank = float_of_int max_rank_i in
  let baselog = ref (logbin ds_rank) in
  let children = ref [] in
  let child = ref ds_rank in
  while !child < max_rank do
    baselog := !baselog +. 1.0;
    child := ds_rank +. 2. ** !baselog;
    children := (int_of_float !child) :: !children;
    Log.debug "%d %f" my_rank_i !child
  done;
  !children

(* setup data server *)
let ds_log_fn = ref ""
let ds_host = ref "localhost"
let ds_port_in = ref Utils.default_ds_port_in
let cli_host = ref ""
let cli_port_in = ref Utils.default_cli_port_in
let my_rank = ref Utils.uninitialized
let machine_file = ref ""
let mds_host = ref "localhost"
let mds_port_in = ref Utils.default_mds_port_in
let chunk_size = ref Utils.default_chunk_size (* DAFT global constant *)
let local_state = ref FileSet.empty
let data_store_root = ref ""
let local_node = ref (Node.dummy ()) (* this node *)

let abort msg =
  Log.fatal "%s" msg;
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
  let last_chunk_size_opt =
    if last_chunk_size_i64 <> Int64.zero then Some last_chunk_size_i64
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
  else begin
    if not (Sys.file_exists fn) then
      Fetch_file_cmd_nack (fn, No_such_file)
    else begin
      if Sys.is_directory fn then
        Fetch_file_cmd_nack (fn, Is_directory)
      else FU.(
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
            let all_chunks =
              File.all_chunks nb_chunks last_chunk_size !local_node
            in
            let new_file = File.create fn size nb_chunks all_chunks in
            local_state := FileSet.add new_file !local_state;
            Fetch_file_cmd_ack fn
          end)
    end
  end

(* same return type than add_file, to keep the protocol small *)
let extract_file (src_fn: Types.filename) (dst_fn: Types.filename): ds_to_cli =
  if FileSet.contains_fn src_fn !local_state then
    if Utils.file_or_link_exists dst_fn then
      Fetch_file_cmd_nack (dst_fn, Already_here)
    else
      let dest_dir = Fn.dirname dst_fn in
      let in_data_store = fn_to_path src_fn in
      (* create all necessary parent dirs *)
      FU.mkdir ~parent:true ~mode:0o700 dest_dir;
      (* only soft link for the moment since it's fast *)
      Unix.symlink in_data_store dst_fn;
      Fetch_file_cmd_ack dst_fn
  else
    Fetch_file_cmd_nack (src_fn, No_such_file)

exception Fatal (* throw this when we are doomed *)

let send_to int2node ds_rank something =
  (* enforce ds_rank bounds because array access soon *)
  if Utils.out_of_bounds ds_rank int2node then
    begin
      Log.fatal "send_to: out of bounds rank: %d" ds_rank;
      raise Fatal
    end
  else match int2node.(ds_rank) with
    | (_node, Some to_ds_i) -> Socket.send to_ds_i something
    | (_, None) ->
      begin
        Log.fatal "send_to: no socket for DS %d" ds_rank;
        raise Fatal
      end

(* retrieve a chunk from disk *)
let retrieve_chunk (fn: string) (chunk_id: int): string =
  try (* 1) check we have this file *)
    let file = FileSet.find_fn fn !local_state in
    try (* 2) check we have this chunk *)
      let chunk = File.find_chunk_id chunk_id file in
      (* 3) seek to it *)
      let local_file = fn_to_path fn in
      Utils.with_in_file_descr local_file (fun input ->
          let offset = chunk_id * !chunk_size in
          assert(Unix.(lseek input offset SEEK_SET) = offset);
          let curr_chunk_size = match Chunk.get_size chunk with
            | None -> !chunk_size
            | Some s ->
              let res = Int64.to_int s in
              assert(res > 0 && res < !chunk_size);
              res
          in
          (* WARNING: data copy here and allocation of
                      a fresh buffer each time *)
          let buff = String.create curr_chunk_size in
          Utils.really_read input buff curr_chunk_size;
          buff
        )
    with Not_found ->
      begin
        Log.fatal "retrieve_chunk: no such chunk: fn: %s id: %d" fn chunk_id;
        raise Fatal
      end
  with Not_found ->
    begin
      Log.fatal "retrieve_chunk: no such file: %s" fn;
      raise Fatal
    end

let send_chunk to_rank int2node fn chunk_id is_last chunk_data =
  let chunk_msg = DS_to_DS (Chunk (fn, chunk_id, is_last, chunk_data)) in
  send_to int2node to_rank chunk_msg

let bcast_chunk to_ranks int2node fn chunk_id is_last root_rank step_num chunk_data =
  let bcast_chunk_msg =
    DS_to_DS
      (Bcast_chunk (fn, chunk_id, is_last, chunk_data, root_rank, step_num))
  in
  List.iter (fun to_rank ->
      send_to int2node to_rank bcast_chunk_msg
    ) to_ranks

let store_chunk to_mds to_cli fn chunk_id is_last data =
  let size = String.length data in
  let size64 = Int64.of_int size in
  let curr_chunk_size =
    if size <> !chunk_size then Some size64 else None
  in
  if not is_last && Option.is_some curr_chunk_size then
    Log.error "store_chunk: invalid chunk size: fn: %s id: %d size: %d"
      fn chunk_id size
  else begin
    (* 1) write chunk at offset *)
    let offset = chunk_id * !chunk_size in
    let local_file = fn_to_path fn in
    let dest_dir = Fn.dirname local_file in
    (* mkdir creates all necessary parent dirs *)
    FU.mkdir ~parent:true ~mode:0o700 dest_dir;
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
    let curr_chunk =
      File.Chunk.create chunk_id curr_chunk_size !local_node
    in
    (* only once we receive the last chunk can we finalize the file's
       description in local_state *)
    let new_file =
      if is_last then (* finalize file description *)
        let full_size = Int64.(of_int offset + size64) in
        let total_chunks = chunk_id + 1 in
        let chunks = File.get_chunks file in
        File.create fn full_size total_chunks chunks
      else
        File.add_chunk file curr_chunk;
    in
    local_state := FileSet.update new_file !local_state;
    (* 3) notify MDS *)
    Socket.send to_mds
      (DS_to_MDS (Chunk_ack (fn, chunk_id, !my_rank)));
    (* 4) notify CLI if all file chunks have been received
          and fix file perms in the local datastore *)
    let curr_chunks = File.get_chunks new_file in
    let nb_chunks = File.get_nb_chunks new_file in
    if ChunkSet.cardinal curr_chunks = nb_chunks then
      begin
        Unix.chmod local_file 0o400;
        match to_cli with
        | None -> () (* in case of broadcast: no feedback to CLI *)
        | Some cli_sock ->
          Socket.send cli_sock (DS_to_CLI (Fetch_file_cmd_ack fn))
      end
  end

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  ds_host := hostname ();
  (* options parsing *)
  Arg.parse
    [ "-cs", Arg.Set_int chunk_size, "<size> file chunk size";
      "-l", Arg.Set_string ds_log_fn, "<filename> where to log";
      "-cli", Arg.String (Utils.set_host_port cli_host cli_port_in),
      "<host:port> CLI";
      "-p", Arg.Set_int ds_port_in, "<port> where to listen";
      "-r", Arg.Set_int my_rank, "<rank> rank among other data nodes";
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
  if !my_rank = Utils.uninitialized then abort "-r is mandatory";
  if !ds_port_in = Utils.uninitialized then abort "-p is mandatory";
  if !cli_host = "" then abort "-cli is mandatory";
  if !cli_port_in = Utils.uninitialized then abort "-cli is mandatory";
  if !machine_file = "" then abort "-m is mandatory";
  let ctx = ZMQ.Context.create () in
  let int2node = Utils.data_nodes_array !machine_file in
  (* create a push socket for each DS, except the current one because we
     will never send to self *)
  A.iteri (fun i (node, _sock) ->
      if i <> !my_rank then
        let sock =
          Utils.(zmq_socket Push ctx (Node.get_host node) (Node.get_port node))
        in
        A.set int2node i (node, Some sock)
    ) int2node;
  let nb_nodes = A.length int2node in
  Log.info "read %d host(s)" nb_nodes;
  local_node := Node.create !my_rank !ds_host !ds_port_in;
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
  Socket.send to_mds (DS_to_MDS (Join_push !local_node));
  try (* loop on messages until quit command *)
    let not_finished = ref true in
    while !not_finished do
      let message' = Socket.receive incoming in
      match message' with
      | None -> Log.warn "junk"
      | Some message ->
        match message with
        | MDS_to_DS (Send_to_req (to_rank, fn, chunk_id, is_last)) ->
          begin
            Log.debug "got Send_to_req";
            let chunk_data = retrieve_chunk fn chunk_id in
            send_chunk to_rank int2node fn chunk_id is_last chunk_data
          end
        | MDS_to_DS Quit_cmd ->
          Log.debug "got Quit_cmd";
          let _ = Log.info "got Quit" in
          not_finished := false
        | MDS_to_DS (Add_file_ack fn) ->
          Log.debug "got Add_file_ack";
          (* forward the good news to the CLI *)
          Socket.send to_cli
            (DS_to_CLI (Fetch_file_cmd_ack fn))
        | MDS_to_DS (Add_file_nack fn) ->
          Log.debug "got Add_file_nack";
          Log.warn "datastore: no rollback for %s" fn;
          (* quick and dirty but maybe OK: only rollback local state's view *)
          local_state := FileSet.remove_fn fn !local_state;
          (* forward nack to CLI *)
          Socket.send to_cli
            (DS_to_CLI (Fetch_file_cmd_nack (fn, Already_here)))
        | DS_to_DS (Chunk (fn, chunk_id, is_last, data)) ->
          Log.debug "got Chunk";
          store_chunk to_mds (Some to_cli) fn chunk_id is_last data
        | CLI_to_DS (Fetch_file_cmd_req (fn, Local)) ->
          Log.debug "got Fetch_file_cmd_req:Local";
          let res = add_file fn in
          begin match res with
            | Fetch_file_cmd_ack fn ->
              (* notify MDS about this new file *)
              let file = FileSet.find_fn fn !local_state in
              let add_file_req = Add_file_req (!my_rank, file) in
              Socket.send to_mds (DS_to_MDS add_file_req)
            | Fetch_file_cmd_nack (fn, err) ->
              Socket.send to_cli
                (DS_to_CLI (Fetch_file_cmd_nack (fn, err)))
            | Bcast_file_ack -> assert(false)
          end
        | CLI_to_DS (Bcast_file_cmd_req fn) ->
          Log.debug "got Bcast_file_cmd_req"; (* FBR: factor this code *)
          let res = add_file fn in
          begin match res with
            | Fetch_file_cmd_nack (fn, Already_here) (* allow bcast after put *)
            | Fetch_file_cmd_ack fn ->
	      (* here starts the true business of the broadcast *)
              let file = FileSet.find_fn fn !local_state in
              let last_cid = (File.get_nb_chunks file) - 1 in
              let algo = `Seq in
              begin match algo with
                | `Seq ->
                  begin
                    let bcast_file_req = Bcast_file_req (!my_rank, file) in
                    (* broadcasting a file is also adding it: we cannot start
                       broadcasting right here: we first need the MDS to allow
                       this new file *)
	            Socket.send to_mds (DS_to_MDS bcast_file_req);
                    (* unlock CLI *)
                    Socket.send to_cli (DS_to_CLI Bcast_file_ack)
                  end
                | `Amoeba ->
                  match Amoeba.fork (!my_rank, 0, !my_rank, nb_nodes) with
                  | [], _ -> () (* job done *)
                  | (to_ranks, (root_rank, step_num, _, _)) ->
                    match to_ranks with
                    | [_] | [_; _] -> (* one or two successors *)
                      for chunk_id = 0 to last_cid do
                        let chunk_data = retrieve_chunk fn chunk_id in
                        let is_last = (chunk_id = last_cid) in
                        bcast_chunk to_ranks int2node fn chunk_id is_last
                          root_rank step_num chunk_data
                      done
                    | _ -> assert(false)
              end
            | Fetch_file_cmd_nack (fn, err) ->
              Socket.send to_cli
                (DS_to_CLI (Fetch_file_cmd_nack (fn, err)))
            | Bcast_file_ack -> assert(false)
	  end
        | CLI_to_DS (Fetch_file_cmd_req (fn, Remote)) ->
          Log.debug "got Fetch_file_cmd_req:Remote";
          (* finish quickly in case file is already present locally *)
          if FileSet.contains_fn fn !local_state then
            let _ = Log.info "%s already here" fn in
            Socket.send to_cli
              (DS_to_CLI (Fetch_file_cmd_ack fn))
          else (* forward request to MDS *)
            Socket.send to_mds
              (DS_to_MDS (Fetch_file_req (Node.get_rank !local_node, fn)))
        | CLI_to_DS (Extract_file_cmd_req (src_fn, dst_fn)) ->
          let res = extract_file src_fn dst_fn in
          Socket.send to_cli (DS_to_CLI res)
	| DS_to_DS
            (Bcast_chunk
               (fn, chunk_id, is_last, chunk_data, root_rank, step_num)) ->
          begin
            store_chunk to_mds None fn chunk_id is_last chunk_data;
            match Amoeba.fork (root_rank, step_num, !my_rank, nb_nodes) with
            | ([], _) -> () (* job done *)
            | (to_ranks, (root_rank, step_num, _, _)) ->
              match to_ranks with
              | [_] | [_; _] -> (* one or two successors *)
                bcast_chunk to_ranks int2node fn chunk_id is_last
                  root_rank step_num chunk_data
              | _ -> assert(false)
          end
    done;
    raise Types.Loop_end;
  with exn -> begin
      (* it's useful to see the local data store state for debug *)
      (* let (_: int) = delete_data_store !data_store_root in *)
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

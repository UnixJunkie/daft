open Batteries
open Printf

module Fn = Filename
module FU = FileUtil
module Logger = Log
module Log = Log.Make(struct let section = "DS" end) (* prefix logs *)
module S = String
module T = Types

(* ALL OPERATIONS ARE SYNCHRONOUS *)

(* setup data server *)
let ds_log_fn = ref ""
let ds_host = Utils.hostname ()
let ds_port = ref 0
let mds_host = ref ""
let mds_port = ref (-1)
let chunk_size = ref (-1) (* must be set on startup and same for all DSs *)
let local_state = ref T.FileSet.empty
let data_store_root = ref ""

(* create local data store with unix UGO rights 700
   we take into account the port so that several DSs can be started on one
   host at the same time, for tests *)
let create_data_store (host: string) (port: int): string =
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root = sprintf "%s/%s:%d.ds" tmp_dir host port in
  Unix.mkdir data_store_root 0o700;
  Log.info "I store in %s" data_store_root;
  data_store_root

(* destroy a data store *)
let delete_data_store (ds: string): int =
  Sys.command ("rm -rf " ^ ds)

(* FBR: maybe I should really create a type to take into account all
        possible errors instead of different strings in Error *)
let add_file (fn: string): T.answer =
  if T.FileSet.contains_fn fn !local_state
  then T.Error ("already here: " ^ fn)
  else
    let stat = FU.stat fn in
    let size = FU.byte_of_size stat.size in
    if Sys.is_directory fn
    then T.Error ("directory: " ^ fn)
    else
      let fn = if S.starts_with fn "/" (* chop leading '/' if any *)
               then S.lchop ~n:1 fn
               else fn
      in
      (* FBR: create all necessary dirs in the local data store *)
      let dest_fn = !data_store_root ^ "/" ^ fn in
      FU.cp ~follow:FU.Follow ~force:FU.Force ~recurse:false [fn] dest_fn;
      (* check cp succeeded *)
      let stat' = FU.stat dest_fn in
      if stat'.size <> stat.size
      then T.Error ("cp failed: " ^ fn)
      else begin (* update local state *)
        (* n.b. we keep the stat from the original file *)
        let new_file = T.create_managed_file fn size stat in
        local_state := T.FileSet.add new_file !local_state;
        T.Ok
       end

let main () =
  (* setup logger *)
  Logger.set_log_level Logger.DEBUG;
  Logger.set_output Legacy.stdout;
  Logger.color_on ();
  (* options parsing *)
  Arg.parse
    [ "-cs", Arg.Set_int chunk_size, "<size> file chunk size";
      "-l", Arg.Set_string ds_log_fn, "<filename> where to log";
      "-p", Arg.Set_int ds_port, "<port> where to listen";
      "-s", Arg.Set_string mds_host, "<server> hostname";
      "-sp", Arg.Set_int mds_port, "<port> server port" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  (* check options *)
  if !ds_log_fn <> "" then begin (* don't log anything before that *)
    let log_out = Legacy.open_out !ds_log_fn in
    Logger.set_output log_out
  end;
  if !mds_host = "" then begin
    Log.fatal "-s is mandatory";
    exit 1;
  end;
  if !chunk_size = -1 then begin
    Log.fatal "-cs is mandatory";
    exit 1;
  end;
  if !mds_port = -1 then begin
    Log.fatal "-sp is mandatory";
    exit 1;
  end;
  Log.info "Will connect to %s:%d" !mds_host !mds_port;
  data_store_root := create_data_store !mds_host !mds_port;
  delete_data_store !data_store_root
;;

main ()

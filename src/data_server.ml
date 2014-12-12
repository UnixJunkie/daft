open Batteries
open Printf

module Fn = Filename

(* create local data store with unix UGO rights 700 *)
let create_data_store (host: string) (port: int): string =
  let tmp_dir = Fn.get_temp_dir_name () in
  let data_store_root = sprintf "%s/%s:%d.ds" tmp_dir host port in
  Unix.mkdir data_store_root 0o700;
  Log.info "I store in %s" data_store_root;
  data_store_root

(* destroy a data store *)
let delete_data_store (ds: string): int =
  Sys.command ("rm -rf " ^ ds)

let main () =
  (* setup logger *)
  Log.set_log_level Log.DEBUG;
  Log.set_output Legacy.stdout;
  Log.color_on ();
  (* setup data server *)
  let port = ref 0 in
  let host = Utils.hostname () in
  let data_store = create_data_store host !port in
  exit (delete_data_store data_store)
;;

main()

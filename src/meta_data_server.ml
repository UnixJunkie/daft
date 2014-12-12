open Batteries
open Printf

let main () =
  (* setup logger *)
  Log.set_log_level Log.DEBUG;
  Log.set_output Legacy.stdout;
  Log.color_on ();
  (* setup MDS *)
  let port = ref 0 in
  let host = Utils.hostname () in
  Arg.parse
    [ "-p", Arg.Set_int port, "port where to listen" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  Log.info "MDS: %s:%d" host !port;
  (* start all DSs *)
  failwith "not implemented yet"
  (* wait for commands *)
;;

main ()

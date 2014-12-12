open Batteries
open Printf

module L = List

let parse_machine_line (l: string): string * int =
  let hostname, port = String.split l ":" in
  (hostname, int_of_string port)

let parse_machine_file (fn: string): (string * int) list =
  let res = ref [] in
  Utils.with_in_file fn
    (fun input ->
       try
         while true do
           res := (parse_machine_line (Legacy.input_line input)) :: !res
         done
       with End_of_file -> ()
    );
  L.rev !res

let main () =
  (* setup logger *)
  Log.set_log_level Log.DEBUG;
  Log.set_output Legacy.stdout;
  Log.color_on ();
  (* setup MDS *)
  let port = ref 0 in
  let host = Utils.hostname () in
  let machine_file = ref "" in
  Arg.parse
    [ "-p", Arg.Set_int port, "port where to listen";
      "-m", Arg.Set_string machine_file,
      "machine_file list of host:port (one per line)" ]
    (fun arg -> raise (Arg.Bad ("Bad argument: " ^ arg)))
    (sprintf "usage: %s <options>" Sys.argv.(0))
  ;
  Log.info "MDS: %s:%d" host !port;
  let machines = parse_machine_file !machine_file in
  Log.info "MDS: read %d hosts" (L.length machines);
  (* start all DSs *)
  failwith "not implemented yet"
  (* wait for commands *)
;;

main ()


(* like `cmd` in shell
   FBR: use the one in batteries upon next release *)
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

open Batteries (* everything before uses Legacy IOs (fast) *)

let hostname (): string =
  let stat, res = run_and_read "hostname -f" in
  assert(stat = Unix.WEXITED 0);
  String.strip res (* rm trailing '\n' *)

(* wrappers around ZMQ push and pull sockets to statically enforce their
   correct usage *)

open Types.Protocol

module type Msg_pair =
sig
  type from_t
  type to_t
end

module Make (Pair: Msg_pair) = struct

  (* generic send (private) *)
  let send (compression_flag: bool) (sock: [> `Push] ZMQ.Socket.t) m: unit =
    let encode m: string =
      let to_send = Marshal.to_string m [Marshal.No_sharing] in
      let before_size = float_of_int (String.length to_send) in
      if compression_flag then
        let res = compress to_send in
        let after_size = float_of_int (String.length res) in
        Log.debug "z ratio: %.2f" (after_size /. before_size);
        res
      else
        to_send
    in
    let encoded = encode m in
    ZMQ.Socket.send sock encoded

  (* generic receive (private) *)
  let receive (compression_flag: bool) (sock: [> `Pull] ZMQ.Socket.t) =
    let decode (s: string) =
      let received =
        if compression_flag then uncompress s
        else s
      in
      Marshal.from_string received 0
    in
    let encoded = ZMQ.Socket.recv sock in
    decode encoded

end

module CLI_socket = Make (struct type from_t = from_cli type to_t = to_cli end)
module MDS_socket = Make (struct type from_t = from_mds type to_t = to_mds end)
module  DS_socket = Make (struct type from_t = from_ds  type to_t = to_ds  end)

(* wrappers around ZMQ push and pull sockets to statically enforce their
   correct usage *)

open Types.Protocol

let compression_flag = false
let encryption_flag = false
let signature_flag = true (* FBR: doing this one *)

let encode (m: 'a): string =
  let to_send = Marshal.to_string m [Marshal.No_sharing] in
  let before_size = float_of_int (String.length to_send) in
  if compression_flag then
    let res = compress to_send in
    let after_size = float_of_int (String.length res) in
    Log.debug "z ratio: %.2f" (after_size /. before_size);
    res
  else
    to_send

let decode (s: string): 'a =
  let received =
    if compression_flag then uncompress s
    else s
  in
  Marshal.from_string received 0

module CLI_socket = struct

  let send
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_cli): unit =
    (* marshalling + type translation so that message is OK to unmarshall
       at receiver's side *)
    let translate_type: from_cli -> string = function
      | CLI_to_MDS x ->
        let to_send: to_mds = CLI_to_MDS x in
        encode to_send
      | CLI_to_DS x ->
        let to_send: to_ds = CLI_to_DS x in
        encode to_send
    in
    ZMQ.Socket.send sock (translate_type m)

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_cli =
    decode (ZMQ.Socket.recv sock)

end

module MDS_socket = struct

  let send
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_mds): unit =
    let translate_type: from_mds -> string = function
      | MDS_to_DS x ->
        let to_send: to_ds = MDS_to_DS x in
        encode to_send
      | MDS_to_CLI x ->
        let to_send: to_cli = MDS_to_CLI x in
        encode to_send
    in
    ZMQ.Socket.send sock (translate_type m)

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_mds =
    decode (ZMQ.Socket.recv sock)

end

module DS_socket = struct

  let send
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_ds): unit =
    let translate_type: from_ds -> string = function
      | DS_to_MDS x ->
        let to_send: to_mds = DS_to_MDS x in
        encode to_send
      | DS_to_DS x ->
        let to_send: to_ds = DS_to_DS x in
        encode to_send
      | DS_to_CLI x ->
        let to_send: to_cli = DS_to_CLI x in
        encode to_send
    in
    ZMQ.Socket.send sock (translate_type m)

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_ds =
    decode (ZMQ.Socket.recv sock)

end

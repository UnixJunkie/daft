(* wrappers around ZMQ push and pull sockets to statically enforce their
   correct usage *)

open Types.Protocol

module Crypto = Cryptokit.Block
module Padd = Cryptokit.Padding

(* FBR: one day, position those flags with (cppo) preprocessor directives; e.g.
#ifdef NO_CRYPTO
let encryption_flag = false
#else
let encryption_flag = true
*)
let compression_flag = false
let encryption_flag  = false
let signature_flag   = false

module Buffer = struct
  type t = { data:           string ;
             mutable offset: int    ;
             mutable length: int    }
  let create data =
    { data; offset = 0; length = String.length data }
  let set_offset buff new_offset =
    assert(buff.offset = 0); (* don't move the offset, just set it *)
    buff.offset <- new_offset;
    buff.length <- buff.length - new_offset
  let to_string buff =
    String.sub buff.data buff.offset buff.length
end

let may_do cond f x default =
  if cond then f x
  else default

let chain f = function
  | None -> None
  | Some x -> Some (f x)

let compress (s: string): string =
  LZ4.Bytes.compress (Bytes.of_string s)

let uncompress (s: Buffer.t): string =
  (* WARNING: data copy *)
  (* FBR: use a BigArray as a buffer if LZ4 can *)
  LZ4.Bytes.decompress ~length:1_572_864 (Buffer.to_string s)

(* FBR: constant default keys for the moment
        in the future they will be asked interactively
        to the user at runtime *)
let signing_key    = "please_sign_this0123456789"
let encryption_key = "please_crypt_this0123456789"

(* prefix the message with its signature
   msg --> signature|msg ; length(signature) = 20B = 160bits *)
let sign (msg: string): string =
  let signing_object =
    assert(String.length signing_key >= 20);
    assert(encryption_key <> signing_key);
    Cryptokit.MAC.hmac_ripemd160 signing_key
  in
  signing_object#add_string msg;
  let signature = signing_object#result in
  assert(String.length signature = 20);
  signature ^ msg

(* optionally return the message without its prefix signature or None
   if the signature is incorrect or anything strange was found *)
let check_sign (msg: string): Buffer.t option =
  let res = Buffer.create msg in
  let n = Buffer.(res.length) in
  if n <= 20 then
    begin
      Log.error "check_sign: message too short: %d" n;
      None
    end
  else
    let prev_sign = String.sub msg 0 20 in
    let signing_object =
      assert(String.length signing_key >= 20);
      assert(encryption_key <> signing_key);
      Cryptokit.MAC.hmac_ripemd160 signing_key
    in
    signing_object#add_substring msg 20 (n - 20);
    let curr_sign = signing_object#result in
    if curr_sign <> prev_sign then begin
      Log.error "check_sign: bad signature";
      None
    end else begin
      Buffer.set_offset res 20;
      Some res
    end

let encrypt_obj =
  assert(String.length encryption_key >= 16); (* 16B = 128bits *)
  assert(encryption_key <> signing_key);
  new Crypto.cipher_padded_encrypt Padd._8000
    (new Crypto.cbc_encrypt
      (new Crypto.blowfish_encrypt encryption_key))

let encrypt (msg: string): string =
  encrypt_obj#put_string msg;
  encrypt_obj#get_string

(* FBR: TODO: full pipeline *)
(* full pipeline: compress then salt then encrypt then sign *)
let encode (m: 'a): string =
  let to_send = Marshal.to_string m [Marshal.No_sharing] in
  let tmp =
    may_do compression_flag
      (fun to_send ->
         let before = float_of_int (String.length to_send) in
         let res = compress to_send in
         let after = float_of_int (String.length res) in
         Log.debug "z ratio: %.2f" (after /. before);
         res
      ) to_send to_send
  in
  may_do signature_flag
    (fun x ->
       let before = float_of_int (String.length x) in
       let res = sign x in
       let after = float_of_int (String.length res) in
       Log.debug "s ratio: %.2f" (after /. before);
       res
    ) tmp tmp

(* FBR: TODO: full pipeline *)
(* full pipeline: check signature then decrypt then remove salt then uncompress *)
let decode (s: string): 'a option =
  let maybe_signed = may_do signature_flag check_sign s (Some (Buffer.create s)) in
  let message =
    may_do compression_flag (chain uncompress)
      maybe_signed (chain Buffer.to_string maybe_signed)
  in
  chain (fun x -> Marshal.from_string x 0) message

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

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_cli option =
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

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_mds option =
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

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_ds option =
    decode (ZMQ.Socket.recv sock)

end

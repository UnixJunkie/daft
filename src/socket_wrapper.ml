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

(* DAFT modes       z     c      s   *)
let fast_mode    = (true, false, false)
let regular_mode = (true, false, true )
let parano_mode  = (true, true , true )

let compression_flag, encryption_flag, signature_flag = regular_mode

let may_do cond f x =
  if cond then f x else x

let chain f = function
  | None -> None
  | Some x -> f x

let ignore_first x y =
  ignore(x);
  y

let compress (s: string): string =
  LZ4.Bytes.compress (Bytes.of_string s)

let uncompress (s: string option): string option =
  match s with
  | None -> None
  | Some compressed ->
    (* FBR: use a BigArray as a buffer if LZ4 can *)
    try Some (LZ4.Bytes.decompress ~length:1_572_864 compressed)
    with LZ4.Corrupted -> ignore_first (Log.error "uncompress: corrupted") None

(* FBR: constant default keys for the moment
        in the future they will be asked interactively
        to the user at runtime *)
let signing_key    = "please_sign_this0123456789"
let encryption_key = "please_crypt_this0123456789"

let create_signer () =
  assert(String.length signing_key >= 20);
  assert(encryption_key <> signing_key);
  Cryptokit.MAC.hmac_ripemd160 signing_key

(* prefix the message with its signature
   msg --> signature|msg ; length(signature) = 20B = 160bits *)
let sign (msg: string): string =
  let signer = create_signer () in
  signer#add_string msg;
  let signature = signer#result in
  assert(String.length signature = 20);
  signature ^ msg

(* optionally return the message without its prefix signature or None
   if the signature is incorrect or anything strange was found *)
let check_sign (s: string option): string option =
  match s with
  | None -> None
  | Some msg ->
    let n = String.length msg in
    if n <= 20 then
      ignore_first (Log.error "check_sign: message too short: %d" n) None
    else
      let prev_sign = String.sub msg 0 20 in
      let signer = create_signer () in
      let m = n - 20 in
      signer#add_substring msg 20 m;
      let curr_sign = signer#result in
      if curr_sign <> prev_sign then
        ignore_first (Log.error "check_sign: bad signature") None
      else
        Some (String.sub msg 20 m)

let enigma =
  assert(String.length encryption_key >= 16); (* 16B = 128bits *)
  assert(encryption_key <> signing_key);
  (new Crypto.cipher_padded_encrypt Padd.length
    (new Crypto.cbc_encrypt
      (new Crypto.blowfish_encrypt encryption_key)))

let turing =
  assert(String.length encryption_key >= 16); (* 16B = 128bits *)
  assert(encryption_key <> signing_key);
  (new Crypto.cipher_padded_decrypt Padd.length
    (new Crypto.cbc_decrypt
      (new Crypto.blowfish_decrypt encryption_key)))

let encrypt (msg: string): string =
  enigma#put_string msg;
  enigma#get_string

let decrypt = function
  | None -> None
  | Some msg ->
    turing#put_string msg;
    Some (turing#get_string)

(* FBR: TODO: SALT *)
(* full pipeline: compress --> salt --> encrypt --> sign *)
let encode (m: 'a): string =
  let plain_text = Marshal.to_string m [Marshal.No_sharing] in
  let maybe_compressed =
    may_do compression_flag
      (fun x ->
         let before = String.length x in
         let res = compress x in
         let after = String.length res in
         Log.debug "z: %d -> %d" before after;
         res
      ) plain_text
  in
  let maybe_encrypted =
    may_do encryption_flag
      (fun x ->
         let before = String.length x in
         let res = encrypt x in
         let after = String.length res in
         Log.debug "c: %d -> %d" before after;
         res
      ) maybe_compressed
  in
  may_do signature_flag
    (fun x ->
       let before = String.length x in
       let res = sign x in
       let after = String.length res in
       Log.debug "s: %d -> %d" before after;
       res
    ) maybe_encrypted

let unmarshall (x: string): 'a option =
  Some (Marshal.from_string x 0)

(* FBR: TODO: REMOVE SALT *)
(* full pipeline: check signature --> decrypt --> remove salt --> uncompress *)
let decode (s: string): 'a option =
  let sign_OK = may_do signature_flag check_sign (Some s) in
  let cipher_OK = may_do encryption_flag decrypt sign_OK in
  let compression_OK = may_do compression_flag uncompress cipher_OK in
  chain unmarshall compression_OK

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

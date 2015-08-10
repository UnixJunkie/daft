(* wrappers around ZMQ push and pull sockets to statically enforce their
   correct usage *)

open Types.Protocol

module Nonce_store = Types.Nonce_store
module RNG = Types.RNG
module Node = Types.Node

let encryption_flag, signature_flag = true, true

let may_do cond f x =
  if cond then f x else x

let chain f = function
  | None -> None
  | Some x -> f x

(* FBR: use a BigArray as a buffer; since LZ4 can nowadays *)
let buff_size = 1_572_864
let buffer = Bytes.create buff_size

(* hot toggable compression: never inflate messages *)
let compress (s: string): string =
  let before = String.length s in
  let after = LZ4.Bytes.compress_into (Bytes.of_string s) buffer in
  if after >= before then
    "0" ^ s (* flag as not compressed and keep as is *)
  else
    let res = Bytes.sub_string buffer 0 after in
    (* Log.debug "z: %d -> %d" before after; *)
    ("1" ^ res) (* flag as compressed *)

exception Too_short
exception Invalid_first_char

let uncompress (s: string option): string option =
  match s with
  | None -> None
  | Some maybe_compressed ->
    try
      let n = String.length maybe_compressed in
      if n < 2 then
        raise Too_short
      else
        let body = String.sub maybe_compressed 1 (n - 1) in
        if String.get maybe_compressed 0 = '0' then (* not compressed *)
          Some body
        else if String.get maybe_compressed 0 = '1' then (* compressed *)
          let after =
            LZ4.Bytes.decompress_into (Bytes.of_string body) buffer
          in
          Some (Bytes.sub_string buffer 0 after)
      else
        raise Invalid_first_char
    with
    | LZ4.Corrupted ->
      Utils.ignore_first (Log.error "uncompress: corrupted") None
    | Too_short ->
      Utils.ignore_first (Log.error "uncompress: too short") None
    | Invalid_first_char ->
      Utils.ignore_first (Log.error "uncompress: invalid first char") None

(* FBR: constant default keys for the moment
        in the future they will be asked interactively
        to the user at runtime *)
let signing_key    = "sadkl;vjawfgipabvaskd;jf"
let encryption_key = "asdfasdhfklarjbvfawejkna"

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
      Utils.ignore_first (Log.error "check_sign: message too short: %d" n) None
    else
      let prev_sign = String.sub msg 0 20 in
      let signer = create_signer () in
      let m = n - 20 in
      signer#add_substring msg 20 m;
      let curr_sign = signer#result in
      if curr_sign <> prev_sign then
        Utils.ignore_first (Log.error "check_sign: bad signature") None
      else
        Some (String.sub msg 20 m)

let encrypt (msg: string): string =
  let enigma =
    new Cryptokit.Block.cipher_padded_encrypt Cryptokit.Padding.length
      (new Cryptokit.Block.cbc_encrypt
        (new Cryptokit.Block.blowfish_encrypt encryption_key))
  in
  enigma#put_string msg;
  enigma#finish;
  enigma#get_string

let decrypt (s: string option): string option =
  match s with
  | None -> None
  | Some msg ->
    let turing =
      new Cryptokit.Block.cipher_padded_decrypt Cryptokit.Padding.length
        (new Cryptokit.Block.cbc_decrypt
          (new Cryptokit.Block.blowfish_decrypt encryption_key))
    in
    turing#put_string msg;
    turing#finish;
    Some turing#get_string

(* full pipeline: compress --> salt --> nonce --> encrypt --> sign *)
let encode (compression_flag: bool) (sender: Node.t) (m: 'a): string =
  let no_sharing = [Marshal.No_sharing] in
  let plain_text = Marshal.to_string m no_sharing in
  let maybe_compressed' = may_do compression_flag compress plain_text in
  let maybe_encrypted =
    may_do encryption_flag
      (fun msg ->
         let salt = RNG.int64 Int64.max_int in
         (* Log.debug "enc. salt = %s" (Int64.to_string salt); *)
         let nonce = Nonce_store.fresh sender in
         (* Log.debug "enc. nonce = %s" nonce; *)
         let s_n_mc = (salt, nonce, msg) in
         let to_encrypt = Marshal.to_string s_n_mc no_sharing in
         let res = encrypt to_encrypt in
         (* Log.debug "c: %d -> %d" (String.length msg) (String.length res); *)
         res
      ) maybe_compressed'
  in
  may_do signature_flag
    (fun x ->
       let res = sign x in
       (* Log.debug "s: %d -> %d" (String.length x) (String.length res); *)
       res
    ) maybe_encrypted

(* full pipeline:
   check sign --> decrypt --> check nonce --> rm salt --> uncompress *)
let decode (compression_flag: bool) (s: string): 'a option =
  let sign_OK = may_do signature_flag check_sign (Some s) in
  let cipher_OK' = may_do encryption_flag decrypt sign_OK in
  match cipher_OK' with
  | None -> None
  | Some str ->
    let maybe_compressed =
      if not encryption_flag then
        Some str
      else
        let (_salt, nonce, mc) =
          (Marshal.from_string str 0: Int64.t * string * string)
        in
        (* Log.debug "dec. salt = %s" (Int64.to_string salt); *)
        (* Log.debug "dec. nonce = %s" nonce; *)
        if Nonce_store.is_fresh nonce then
          Some mc
        else
          Utils.ignore_first (Log.warn "nonce already seen: %s" nonce) None
    in
    let compression_OK =
      may_do compression_flag uncompress maybe_compressed
    in
    chain (fun x -> Some (Marshal.from_string x 0: 'a)) compression_OK

module CLI_socket = struct

  let send
      ?async:(async_flag = false)
      ?compress:(compression_flag = true)
      (sender: Node.t)
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_cli)
    : unit =
    (* marshalling + type translation so that message is OK to unmarshall
       at receiver's side *)
    let translate_type: from_cli -> string = function
      | CLI_to_MDS x ->
        let to_send: to_mds = CLI_to_MDS x in
        encode compression_flag sender to_send
      | CLI_to_DS x ->
        let to_send: to_ds = CLI_to_DS x in
        encode compression_flag sender to_send
    in
    ZMQ.Socket.send ~block:async_flag sock (translate_type m)

  let receive
      ?uncompress:(compression_flag = true)
      (sock: [> `Pull] ZMQ.Socket.t)
    : to_cli option =
    decode compression_flag (ZMQ.Socket.recv sock)

end

module MDS_socket = struct

  let send
      ?async:(async_flag = false)
      ?compress:(compression_flag = true)
      (sender: Node.t)
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_mds)
    : unit =
    let translate_type: from_mds -> string = function
      | MDS_to_DS x ->
        let to_send: to_ds = MDS_to_DS x in
        encode compression_flag sender to_send
      | MDS_to_CLI x ->
        let to_send: to_cli = MDS_to_CLI x in
        encode compression_flag sender to_send
    in
    ZMQ.Socket.send ~block:async_flag sock (translate_type m)

  let receive
      ?uncompress:(compression_flag = true)
      (sock: [> `Pull] ZMQ.Socket.t)
    : to_mds option =
    decode compression_flag (ZMQ.Socket.recv sock)

end

module DS_socket = struct

  let send
      ?async:(async_flag = false)
      ?compress:(compression_flag = true)
      (sender: Node.t)
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_ds)
    : unit =
    let translate_type: from_ds -> string = function
      | DS_to_MDS x ->
        let to_send: to_mds = DS_to_MDS x in
        encode compression_flag sender to_send
      | DS_to_DS x ->
        let to_send: to_ds = DS_to_DS x in
        encode compression_flag sender to_send
      | DS_to_CLI x ->
        let to_send: to_cli = DS_to_CLI x in
        encode compression_flag sender to_send
    in
    ZMQ.Socket.send ~block:async_flag sock (translate_type m)

  let receive
      ?uncompress:(compression_flag = true)
      (sock: [> `Pull] ZMQ.Socket.t)
    : to_ds option =
    decode compression_flag (ZMQ.Socket.recv sock)

end

(* wrappers around ZMQ push and pull sockets to statically enforce their
   correct usage *)

open Types.Protocol

module Nonce_store = Types.Nonce_store
module Node = Types.Node

(* initially, we were using LZ4 OCaml wrappers.
   We switched to gzip -1 to remove one library dependency *)
let zlib direction (s: string): string =
  let codec = match direction with
    | `Compress -> Cryptokit.Zlib.compress ~level:1 ()
    | `Uncompress -> Cryptokit.Zlib.uncompress ()
  in
  codec#put_string s;
  codec#finish;
  codec#get_string

let flag_as direction (s: string): string =
  let flag = match direction with
    | `Compressed -> "1"
    | `Plain -> "0"
  in
  flag ^ s

(* hot toggable compression: never inflate messages
   or inflate all messages with a one byte header;
   depending on how you look at it ... *)
let compress (s: string): string =
  let original_length = String.length s in
  let compressed = zlib `Compress s in
  let compressed_length = String.length compressed in
  if compressed_length >= original_length then
    flag_as `Plain s
  else
    (* let _ = Log.debug "z: %d -> %d" original_length compressed_length in *)
    flag_as `Compressed compressed

exception Too_short
exception Invalid_first_char of char

let uncompress (maybe_compressed: string): string option =
  try
    let n = String.length maybe_compressed in
    if n < 2 then
      raise Too_short
    else
      let body = String.sub maybe_compressed 1 (n - 1) in
      let flag = String.get maybe_compressed 0 in
      match flag with
      | '0' -> Some body (* plain *)
      | '1' -> Some (zlib `Uncompress body) (* compressed *)
      | c -> raise (Invalid_first_char c)
  with
  | Too_short ->
    Utils.ignore_first (Log.error "uncompress: too short") None
  | Invalid_first_char c ->
    Utils.ignore_first (Log.error "uncompress: invalid first char: %c" c) None

(* options are used to crash at runtime if keys are not setup *)
let (sign_key: string option ref) = ref None
let (cipher_key: string option ref) = ref None

(* NEEDS_SECURITY_REVIEW *)
(* check keys then store them *)
let setup_keys skey ckey =
  if String.length skey < 20 then failwith "length(sign_key) < 20 chars";
  if String.length ckey < 16 then failwith "length(cipher_key) < 16 chars";
  if skey = ckey then failwith "sign_key = cipher_key";
  sign_key := Some skey;
  cipher_key := Some ckey

(* NEEDS_SECURITY_REVIEW *)
let nuke_keys () =
  let skey = BatOption.get !sign_key in
  let ckey = BatOption.get !cipher_key in
  Cryptokit.wipe_string skey;
  Cryptokit.wipe_string ckey

(* NEEDS_SECURITY_REVIEW *)
let get_key = function
  | `Sign ->
    begin match !sign_key with
      | Some key -> key
      | None ->
        let _ = Log.fatal "no sign key setup" in
        exit 1
    end
  | `Cipher ->
    begin match !cipher_key with
      | Some key -> key
      | None ->
        let _ = Log.fatal "no cipher key setup" in
        exit 1
    end

(* signature of the message (length 20B = 160bits) *)
(* NEEDS_SECURITY_REVIEW *)
let sign (msg: string): string =
  let signer = Cryptokit.MAC.hmac_ripemd160 (get_key `Sign) in
  signer#add_string msg;
  let signature = signer#result in
  signer#wipe;
  assert(String.length signature = 20);
  signature

(* optionally return the message without its prefix signature or None
   if the signature is incorrect or anything strange was found *)
(* NEEDS_SECURITY_REVIEW *)
let check_sign (s: string option): string option =
  match s with
  | None -> None
  | Some msg ->
    let n = String.length msg in
    if n <= 21 then (* 20B signature plus at least 1B message *)
      Utils.ignore_first (Log.error "check_sign: message too short: %d" n) None
    else
      let prev_sign = String.sub msg 0 20 in
      let signless = String.sub msg 20 (n - 20) in
      let curr_sign = sign signless in
      if curr_sign <> prev_sign then
        Utils.ignore_first (Log.error "check_sign: bad signature") None
      else
        Some signless

(* NEEDS_SECURITY_REVIEW *)
let encrypt (msg: string): string =
  let enigma =
    new Cryptokit.Block.cipher_padded_encrypt Cryptokit.Padding.length
      (new Cryptokit.Block.cbc_encrypt
        (new Cryptokit.Block.blowfish_encrypt (get_key `Cipher)))
  in
  enigma#put_string msg;
  enigma#finish;
  let res = enigma#get_string in
  enigma#wipe;
  res

(* NEEDS_SECURITY_REVIEW *)
let decrypt (msg: string): string option =
  let turing =
    new Cryptokit.Block.cipher_padded_decrypt Cryptokit.Padding.length
      (new Cryptokit.Block.cbc_decrypt
        (new Cryptokit.Block.blowfish_decrypt (get_key `Cipher)))
  in
  turing#put_string msg;
  turing#finish;
  let res = turing#get_string in
  turing#wipe;
  Some res

(* full pipeline: compress --> salt --> nonce --> encrypt --> sign *)
(* NEEDS_SECURITY_REVIEW *)
let encode
    (rng: Cryptokit.Random.rng)
    (counter: int ref)
    (sender: Node.t)
    (m: 'a): string =
  let plain_text = Marshal.to_string m [Marshal.No_sharing] in
  let maybe_compressed = compress plain_text in
  let salt = String.make 8 '0' in (* 64 bits salt *)
  rng#random_bytes salt 0 8;
  (* let salt_hex = Utils.convert `To_hexa salt in *)
  (* Log.debug "enc. salt = %s" salt_hex; *)
  let nonce = Nonce_store.fresh counter sender in
  (* Log.debug "enc. nonce = %s" nonce; *)
  let s_n_m = salt ^ nonce ^ "|" ^ maybe_compressed in
  let signed = (sign s_n_m) ^ s_n_m in
  encrypt signed

(* full pipeline:
   check sign --> decrypt --> check nonce --> rm salt --> uncompress *)
(* NEEDS_SECURITY_REVIEW *)
let decode (s: string): 'a option =
  let cipher_OK = decrypt s in
  match check_sign cipher_OK with
  | None -> None
  | Some str ->
    (* ignore salt: 8 first bytes *)
      (*
        let salt = String.sub str 0 8 in
        let salt_hex = Utils.convert `To_hexa salt in
        Log.debug "dec. salt = %s" salt_hex;
      *)
    let n = String.length str in
    let nonce_end = String.index_from str 8 '|' in
    assert(nonce_end > 8 && nonce_end < n);
    let nonce = String.sub str 8 (nonce_end - 8) in
    (* Log.debug "dec. nonce = %s" nonce; *)
    if Nonce_store.is_fresh nonce then
      let maybe_compressed = BatString.lchop ~n:(nonce_end + 1) str in
      let uncompressed = uncompress maybe_compressed in
      match uncompressed with
      | None -> None
      | Some u -> Some (Marshal.from_string u 0: 'a)
    else
      Utils.ignore_first (Log.warn "nonce already seen: %s" nonce) None

let try_send (sock: [> `Push] ZMQ.Socket.t) (m: string): unit =
  try       ZMQ.Socket.send ~block:false sock m
  with _ -> ZMQ.Socket.send ~block:true  sock m

module CLI_socket = struct

  let send
      (rng: Cryptokit.Random.rng)
      (counter: int ref)
      (sender: Node.t)
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_cli)
    : unit =
    (* marshalling + type translation so that message is OK to unmarshall
       at receiver's side *)
    let translate_type: from_cli -> string = function
      | CLI_to_MDS x ->
        let to_send: to_mds = CLI_to_MDS x in
        encode rng counter sender to_send
      | CLI_to_DS x ->
        let to_send: to_ds = CLI_to_DS x in
        encode rng counter sender to_send
    in
    try_send sock (translate_type m)

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_cli option =
    decode (ZMQ.Socket.recv sock)

end

module MDS_socket = struct

  let send
      (rng: Cryptokit.Random.rng)
      (counter: int ref)
      (sender: Node.t)
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_mds)
    : unit =
    let translate_type: from_mds -> string = function
      | MDS_to_DS x ->
        let to_send: to_ds = MDS_to_DS x in
        encode rng counter sender to_send
      | MDS_to_CLI x ->
        let to_send: to_cli = MDS_to_CLI x in
        encode rng counter sender to_send
    in
    try_send sock (translate_type m)

  let receive (sock: [> `Pull] ZMQ.Socket.t): to_mds option =
    decode (ZMQ.Socket.recv sock)

end

module DS_socket = struct

  let send
      (rng: Cryptokit.Random.rng)
      (counter: int ref)
      (sender: Node.t)
      (sock: [> `Push] ZMQ.Socket.t)
      (m: from_ds)
    : unit =
    let translate_type: from_ds -> string = function
      | DS_to_MDS x ->
        let to_send: to_mds = DS_to_MDS x in
        encode rng counter sender to_send
      | DS_to_DS x ->
        let to_send: to_ds = DS_to_DS x in
        encode rng counter sender to_send
      | DS_to_CLI x ->
        let to_send: to_cli = DS_to_CLI x in
        encode rng counter sender to_send
    in
    try_send sock (translate_type m)

  (* send a packet that was already encoded previously *)
  let send_as_is (sock: [> `Push] ZMQ.Socket.t) (m: string): unit =
    try_send sock m

  let receive (sock: [> `Pull] ZMQ.Socket.t): (to_ds * string) option =
    let raw_message = ZMQ.Socket.recv sock in
    let decoded = decode raw_message in
    match decoded with
    | None -> None
    | Some message -> Some (message, raw_message)

end

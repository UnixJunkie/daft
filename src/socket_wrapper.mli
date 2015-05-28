open Types.Protocol

module CLI_socket: sig
  val send: bool -> [> `Push] ZMQ.Socket.t -> from_cli -> unit
  val receive: bool -> [> `Pull] ZMQ.Socket.t -> to_cli
end

module DS_socket: sig
  val send: bool -> [> `Push] ZMQ.Socket.t -> from_ds -> unit
  val receive: bool -> [> `Pull] ZMQ.Socket.t -> to_ds
end

module MDS_socket: sig
  val send: bool -> [> `Push] ZMQ.Socket.t -> from_mds -> unit
  val receive: bool -> [> `Pull] ZMQ.Socket.t -> to_mds
end

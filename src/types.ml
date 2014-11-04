type server = { host: string ;
                port: int    }

type chunk = { rank: int    ;
               data: string }

module Chunk = struct
  type t = chunk
  let compare c1 c2 =
    Int.compare c1.rank c2.rank
end

type msg_type = Raw | Compressed | Signed | Encrypted

type node = Data_node of server | Meta_data_node of server

(* tupe file = (\* set of chunks *\) *)

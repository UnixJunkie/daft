dft
===

### Summary

dft stands for "distributed file transfer".

The final software should provide some of the features that were available in:
https://github.com/HappyCrow/PAR/commit/38276d34ff6fdda051fba2a02a2639f06df44187

### specification

To maintain complexity low, the system will be Write Once Read Many 
(WORM). Files put under the system's control are never deleted nor
modified.
There will be several data servers (one per node) and one meta-data server
(on the node where the user is interacting with the system via
an FTP-like command language).

commands the system should provide:

FBR: maybe I should separate commands acting on meta data from commands
     actually moving the data and that will be used only internally

put # publish a file in the system
    # the file should go to a random data node
    # or to the one specified by the user

diffuse # randomly load balance listed files across data nodes
        # maybe done inside put in fact

mput # put for a directory, or put should be smarter

bcast # send a file to all nodes

get # retrieve a file from the system

mget # get for a directory, or get should be smarter

ls # list files that were published in the system

ls_nodes # list data nodes

ls_chunks # list file chunks

ls_local_chunks # list local file chunks

quit

### Performance requirements

Performance must be better than NFS and should approach what
TakTuk can achieve in terms of performance when distributing
data to cluster nodes.

### Blurry specification for the required libraries

I am looking for the gold standard to write
OCaml modules (not objects) that can talk to each other over the network.

- the target execution environment is composed of
  about 10 Linux workstations. It may switch to 1 or
  2 interconnected clusters in the future (about 512 cores max).
  So, not as large a scale as a company doing big data.
  But, "why have less when you can have more", so this
  use-case should also be supported.
- the system will be used to transfer files of various sizes
  (big files like a few Gb included, tiny ones also).
- pure OCaml code, so JoCaml and CamlP3l are out.
  I don't like so much if there is some C part in the library
  but this is not a show stopper.
- I really dislike syntax extensions (or things that force
  me to do a lot of sysadmin or strange configuration).
  So, user-land only would be great
- preserving type-safety and abstraction as mentioned
  in the [Quicksilver/OCaml paper][www.kb.ecei.tohoku.ac.jp/~sumii/pub/qs.pdf]
  would be cool but not mandatory.
  Ideally, encryption or compression of communications should
  be a user-toggable feature.
- tolerance to partial failures would be nice but not
  mandatory (because the initial target environment is not so error prone
  neither large)
- the project should be actively maintained and preferably used
  in production somewhere ("je n'aime pas essuyer les platres")
- I don't like to use huge frameworks/libraries (j'essaye d'eviter "les
  usines a gaz")

### Useful links

[system programming in OCaml][http://ocamlunix.forge.ocamlcore.org/]

[Async from Janestree's core lib (includes an RPC lib)][https://bitbucket.org/yminsky/ocaml-core/wiki/DummiesGuideToAsync]

[Async install guide][https://bitbucket.org/yminsky/ocaml-core/wiki/InstallationHints]


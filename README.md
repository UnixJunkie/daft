dft
===

### Summary

dft stands for "distributed file transfer".

The final software should provide some of the features that were available in:
https://github.com/HappyCrow/PAR/commit/38276d34ff6fdda051fba2a02a2639f06df44187

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

[Async from Janestree's core lib][https://bitbucket.org/yminsky/ocaml-core/wiki/DummiesGuideToAsync]

[Async install guide][https://bitbucket.org/yminsky/ocaml-core/wiki/InstallationHints]

Async includes an RPC library.

[crypto lib from Xavier Leroy][http://forge.ocamlcore.org/projects/cryptokit/]

[Martin Jambon's biniou][http://mjambon.com/biniou.html]

[Gerd Stolpmann's ocamlnet lib][http://projects.camlcity.org/projects/ocamlnet.html]

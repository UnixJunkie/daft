dft
===

"distributed file transfer"

should provide some of the services that were in:
https://github.com/HappyCrow/PAR/commit/38276d34ff6fdda051fba2a02a2639f06df44187

Here is the blurry specification I sent on caml-list:
---

So, I am looking for the gold standard to write
modules in OCaml that can talk to each other over the network
(not objects, I don't like them so much).

Here are some requirements, in a random order:

- the target execution environment is composed of
  about 10 Linux workstations. It may switch to 1 or
  2 interconnected clusters in the future (about 512 cores max).
  So, not as large a scale as a company doing big data.
- the system will be used to transfer files of various sizes
  (big files like a few Gb included, tiny ones also)
- pure OCaml code, so JoCaml and CamlP3l are out.
  I don't like so much if there is some C part in the library
  but this is not a show stopper.
- I really dislike syntax extensions (or things that force
  me to do a lot of sysadmin strange configuration) so user-land only
  would be great
- preserving type-safety and abstraction as mentioned
  in the Quicksilver/OCaml paper would be cool (
  www.kb.ecei.tohoku.ac.jp/~sumii/pub/qs.pdf
  ) but not mandatory.
  Ideally, encryption or compression of communications should
  be a user-toggable feature.
- tolerance to partial failures would be nice but not
  mandatory (because my target environment is not so error prone
  and not so large)
- the project should be actively maintained and preferably used
  in production somewhere ("je n'aime pas essuyer les platres")
- I don't like to use huge frameworks/libraries (j'essaye d'eviter "les
  usines a gaz") 

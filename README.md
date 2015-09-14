DAFT
====

DAFT Allows File Transfers

### Summary

Distributed file transfer application in user space.

Securely move data files during distributed computational experiments
and provide a global view of all files, even
when there is no distributed filesystem and probably faster than NFS
for large files.

### Provided commands

- put filename: add/publish a given file into the system.

- bcast filename: equivalent to put then send the file to all nodes.

- get filename: retrieve a file previously published into the system.

- ls [-l]: list all files known to the system and optionally where their
  chunks are.

### Installation

Prerequisite: you need opam installed;
c.f. [opam] http://opam.ocaml.org/doc/Install.html.

opam install depext
opam depext ZMQ
opam depext cryptokit
opam depext lz4
opam install dolog batteries fileutils ZMQ cryptokit lz4
make config
make build
make install

### Example user session

TODO

### Specification

To maintain complexity low, the system will be Write Once Read Many 
(WORM). Files put under the system's control are never modified.
There will be several data servers (one per node) and at least
one meta-data server (on the node where the user is interacting with the
system, or remote).

### Dependencies

[batteries] http://batteries.forge.ocamlcore.org/

[zeromq] http://zeromq.org/

[dolog] https://github.com/UnixJunkie/dolog/

[fileutils] http://ocaml-fileutils.forge.ocamlcore.org/

[cryptokit] https://forge.ocamlcore.org/projects/cryptokit/

[lz4] https://github.com/whitequark/ocaml-lz4

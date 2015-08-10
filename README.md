DAFT
====

DAFT Allows File Transfers

### Summary

Distributed file transfer application.

The final software should provide some of the features that were available in:
https://github.com/HappyCrow/PAR/commit/38276d34ff6fdda051fba2a02a2639f06df44187

### Specification

To maintain complexity low, the system will be Write Once Read Many 
(WORM). Files put under the system's control are never deleted nor
modified.
There will be several data servers (one per node) and at least
one meta-data server (on the node where the user is interacting with the
system or remote).

### commands the system should provide:

put list_of_files_or_dirs
diffuse list_of_files_or_dirs
bcast list_of_files_or_dirs
get list_of_files_or_dirs
ls

### Performance requirements

Performance must be better than NFS and should approach what
TakTuk can achieve when distributing data to cluster nodes.

### Dependencies

[batteries] http://batteries.forge.ocamlcore.org/

[zeromq] http://zeromq.org/

[dolog] https://github.com/UnixJunkie/dolog/

[fileutils] http://ocaml-fileutils.forge.ocamlcore.org/

[cryptokit] https://forge.ocamlcore.org/projects/cryptokit/

[lz4] https://github.com/whitequark/ocaml-lz4

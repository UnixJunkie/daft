daft
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

The MDS knows a set of states (each state is a set of files).
The state is associated with a number and this number is incremented each
time a new file chunk is added to the system.
For performance, DS should update their state by asking just the delta
with the last state they knew.

### commands the system should provide:

put list_of_files_or_dirs
    # publish files or directories into the system
    # the file/directory should go to
    # 1) the local data node if any
    # 2) or a random data node
    # 3) to the data node specified by the user
    #    as in [put this_local_file@remote_host]

diffuse list_of_files_or_dirs
    # randomly load balance listed files/directories
    # across data nodes
    # implementation may consist in several calls to put in fact

bcast list_of_files_or_dirs
    # send a file or directory to all data nodes

get list_of_files_or_dirs
    # retrieve a file or directory from the system to the local disk
    # this is the opposite of the put command

ls [options] # list files that were published in the system
             # the listing should include where file chunks physically are
             # options:
             #  -n : list data nodes
             #  -c : list file chunks
             #  -lc: only list local chunks

start host_file # launch via ssh one meta_data_manager (locally by default)
                # and one data_manager on each data node
                # host_file format: list of user@hostname:port lines

quit # stop all data_managers
     # stop the meta_data_manager

### Performance requirements

Performance must be better than NFS and should approach what
TakTuk can achieve when distributing data to cluster nodes.

### Dependencies

All deps are available in OPAM.

[batteries] http://batteries.forge.ocamlcore.org/

[ocamlnet] http://projects.camlcity.org/projects/ocamlnet.html

[dolog] https://github.com/UnixJunkie/dolog/

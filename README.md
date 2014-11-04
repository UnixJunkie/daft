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
There will be several data servers (one per node) and at least
one meta-data server (on the node where the user is interacting with the
system).

commands the system should provide:

FBR: maybe I should separate commands acting on meta data from commands
     actually moving the data and that will be used only internally

put list_of_files_or_dirs
    # publish a file in the system
    # the file should go to a random data node
    # or to the one specified by the user
    # ex: put this_local_file@remote_host:port

diffuse list_of_files_or_dirs
    # randomly load balance listed files across data nodes
    # maybe done inside put in fact

bcast list_of_files_or_dirs
    # send a file or directory to all nodes

get list_of_files_or_dirs
    # retrieve a file or directory from the system

ls # list files that were published in the system
   # the listing should include where the files are physically

ls_nodes # list data nodes

ls_chunks # list file chunks

ls_local_chunks # list local file chunks

start host_file # launch via password-less ssh data_managers on all data nodes
                # one meta_data_manager is launched locally
                # host_file format: list of user@hostname lines

quit # destroy all data_managers, stop the local meta_data_manager

### Performance requirements

Performance must be better than NFS and should approach what
TakTuk can achieve in terms of performance when distributing
data to cluster nodes.

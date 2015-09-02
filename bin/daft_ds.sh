#!/bin/bash

#set -x

# edit DAFT_ROOT to reflect your installation
DAFT_ROOT=~/src/daft

export LD_LIBRARY_PATH=~/usr/zmq-4.0.4/lib:~/usr/lz4-r129/lib

$DAFT_ROOT/daft_ds "$*"

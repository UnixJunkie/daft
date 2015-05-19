#!/bin/bash

#set -x

# copy all DAFT exes to a remote host

if [ "$#" -ne 1 ] ; then
    echo "usage: "$0" [hostname]"
fi

scp \
machines \
dist/build/daft/daft \
dist/build/daft_mds/daft_mds \
dist/build/daft_ds/daft_ds $1:

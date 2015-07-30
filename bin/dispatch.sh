#!/bin/bash

#set -x

# copy all DAFT exes to a remote host

if [ "$#" -ne 1 ] ; then
    echo "usage: "$0" [hostname]"
else
    scp \
        machines \
        bootstrap.sh \
        dist/build/daft/daft \
        dist/build/daft_mds/daft_mds \
        dist/build/daft_ds/daft_ds \
        $1:
fi

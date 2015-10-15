#!/bin/bash

# run local tests interactively

./reset.sh

host=`hostname -f`

echo $host:8083:8080              > machines
echo $host:8084                  >> machines
echo $host:8085                  >> machines
chmod 600 machines

# # interactive use
# ./daft_mds -m machines &
# ./daft_ds  -m machines -p 8083 &
# ./daft_ds  -m machines -p 8084 &
# ./daft_ds  -m machines -p 8085 &
# ./daft     -m machines -i

./daft_mds -v -m machines &

sleep 1s # reading /dev/random takes time

cat machines

./daft_ds -v -m machines -p 8083 &
./daft_ds -v -m machines -p 8084 &
./daft_ds -v -m machines -p 8085 &

sleep 1s
./daft -v -m machines -i

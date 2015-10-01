#!/bin/bash

# to run local tests

#set -x

./reset.sh

host=`hostname -f`

\rm -f machines
touch machines
chmod 600 machines
echo "skey:abcdefghijklmnopqrst" >> machines
echo "ckey:poiuytrewqlkjhgf"     >> machines
echo $host:8083:8080             >> machines
echo $host:8084                  >> machines
echo $host:8085                  >> machines

# # interactive use
# ./daft_mds -m machines &
# ./daft_ds  -m machines -p 8083 &
# ./daft_ds  -m machines -p 8084 &
# ./daft_ds  -m machines -p 8085 &
# ./daft     -m machines -i


./daft_mds -v -m machines &

./daft_ds -v -m machines -p 8083 &
./daft_ds -v -m machines -p 8084 &
./daft_ds -v -m machines -p 8085 &

sleep 1s
./daft -v -m machines -c quit

ps

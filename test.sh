#!/bin/bash

# to run local tests

#set -x

./reset.sh

host=`hostname -f`

echo $host:8083:8080  > machines
echo $host:8084      >> machines
echo $host:8085      >> machines
chmod 644 machines # perms are too wide on purpose

xterm -hold -e "./daft_mds -v -m machines" &
MDS=$!

sleep 1s # reading /dev/random takes time

ls -l machines # perms should be strict now
cat machines

xterm -hold -e "./daft_ds -v -m machines -p 8083" &
DS0=$!
xterm -hold -e "./daft_ds -v -m machines -p 8084" &
DS1=$!
xterm -hold -e "./daft_ds -v -m machines -p 8085" &
DS2=$!

sleep 1s

if [[ $* == *-i* ]]; then
    ./daft -v -m machines -i # interactive test
else
    ./daft -v -m machines quit
fi

sleep 1s
ps

kill $MDS $DS0 $DS1 $DS2


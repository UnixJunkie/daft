# to run local tests

#set -x

./reset.sh

host=`hostname -f`
echo $host:8083:8080 > machines
echo $host:8084 >> machines
echo $host:8085 >> machines

./daft_mds -v -m machines &

./daft_ds -v -m machines -p 8083 &
./daft_ds -v -m machines -p 8084 &
./daft_ds -v -m machines -p 8085 &

sleep 1s
./daft -v -m machines -c quit

ps

reset

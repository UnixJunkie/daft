# to run local tests

#set -x

obuild configure --enable-executable-debugging
obuild build

./reset.sh

./daft_mds.d -m machines -cli `hostname -f`:8000 & # one MDS

./daft_ds.d -m machines -r 0 -mds `hostname -f` -p 8083 -cli `hostname -f`:8000 & # one DS
./daft_ds.d -m machines -r 1 -mds `hostname -f` -p 8084 -cli `hostname -f`:8000 & # one DS
./daft_ds.d -m machines -r 2 -mds `hostname -f` -p 8085 -cli `hostname -f`:8000 & # one DS

# export env. vars. so that the cli invocation is simpler
export DAFT_MDS=`hostname -f`:8082
export DAFT_DS=`hostname -f`:8083

sleep 1s
./daft.d -i

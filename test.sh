# to run local tests

#set -x

function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    \rm -rf /tmp/*.ds # clean previous local datastore
}

reset

OPTS=""

./daft_mds $OPTS -m machines -cli `hostname -f`:8000 & # one MDS

./daft_ds $OPTS -m machines -r 0 -mds `hostname -f` -p 8083 -cli `hostname -f`:8000 & # one DS
./daft_ds $OPTS -m machines -r 1 -mds `hostname -f` -p 8084 -cli `hostname -f`:8000 & # one DS
./daft_ds $OPTS -m machines -r 2 -mds `hostname -f` -p 8085 -cli `hostname -f`:8000 & # one DS

# export env. vars. so that the cli invocation is simpler
export DAFT_MDS=`hostname -f`:8082
export DAFT_DS=`hostname -f`:8083

sleep 1s
./daft $OPTS -c quit

ps

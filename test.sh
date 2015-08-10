# to run local tests

#set -x

function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    pkill daft        # kill CLI
    \rm -rf /tmp/*.ds # clean previous local datastore
}

reset

./daft_mds -m machines & # one MDS

./daft_ds -m machines -r 0 -mds `hostname -f` -p 8083 & # one DS
./daft_ds -m machines -r 1 -mds `hostname -f` -p 8084 & # one DS
./daft_ds -m machines -r 2 -mds `hostname -f` -p 8085 & # one DS

# export env. vars. so that the cli invocation is simpler
export DAFT_MDS=`hostname -f`:8082
export DAFT_DS=`hostname -f`:8083

sleep 1s
./daft -r 0 -p 8000 -c quit

ps

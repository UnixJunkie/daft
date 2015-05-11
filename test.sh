# to run local tests

set -x

function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    \rm -rf /tmp/*.ds # clean previous local datastore
}

reset

OPTS=""
OPTS="-z" # to test on the fly compression of all communications

./daft_mds $OPTS -m machines -cli `hostname -f`:8000 & # one MDS

./daft_ds $OPTS -m machines -r 0 -mds `hostname -f` -p 8083 -cli `hostname -f`:8000 & # one DS
./daft_ds $OPTS -m machines -r 1 -mds `hostname -f` -p 8084 -cli `hostname -f`:8000 & # one DS
./daft_ds $OPTS -m machines -r 2 -mds `hostname -f` -p 8085 -cli `hostname -f`:8000 & # one DS

echo quit | ./daft_cli $OPTS -ds `hostname -f`:8083

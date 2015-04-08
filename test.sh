# to run local tests

function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    \rm -rf /tmp/*.ds # clean previous local datastore
}

reset

./daft_mds -m machines & # one MDS

# 3 local DSs
./daft_ds -m machines -r 0 -mds `hostname -f` -p 8083 & # one DS
./daft_ds -m machines -r 1 -mds `hostname -f` -p 8084 & # one DS
./daft_ds -m machines -r 2 -mds `hostname -f` -p 8085 & # one DS

# automatic tests
sleep 1s
echo quit | ./daft_cli -ds meleze.ens.fr:8083
## interactive tests
# ./daft_cli -ds meleze.ens.fr:8083

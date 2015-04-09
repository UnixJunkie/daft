# to run local tests

function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    \rm -rf /tmp/*.ds # clean previous local datastore
}

reset

./daft_mds -m machines -cli meleze.ens.fr:8000 & # one MDS

# 3 local DSs
./daft_ds -m machines -r 0 -mds `hostname -f` -p 8083 -cli meleze.ens.fr:8000 & # one DS
./daft_ds -m machines -r 1 -mds `hostname -f` -p 8084 -cli meleze.ens.fr:8000 & # one DS
./daft_ds -m machines -r 2 -mds `hostname -f` -p 8085 -cli meleze.ens.fr:8000 & # one DS

# automatic tests
sleep 1s
echo quit | ./daft_cli -ds meleze.ens.fr:8083
## interactive tests
# ./daft_cli -ds meleze.ens.fr:8083

# # to run all processes separately by hand
# FBR: find a way to fire each one in a different xterm so that logs
#      are not interlaced
# ./daft_mds -m machines -cli meleze.ens.fr:8000

# ./daft_ds -m machines -r 0 -mds `hostname -f` -p 8083 -cli meleze.ens.fr:8000
# ./daft_ds -m machines -r 1 -mds `hostname -f` -p 8084 -cli meleze.ens.fr:8000
# ./daft_ds -m machines -r 2 -mds `hostname -f` -p 8085 -cli meleze.ens.fr:8000

# ./daft_cli -ds meleze.ens.fr:8083

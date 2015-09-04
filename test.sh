# to run local tests

#set -x

function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    pkill daft        # kill CLI
    \rm -rf /tmp/*.ds # clean previous local datastore
    \rm -f CLI.msg_counter # force to start a new session
}

reset

./daft_mds -v -m machines &

./daft_ds -v -m machines -p 8083 &
./daft_ds -v -m machines -p 8084 &
./daft_ds -v -m machines -p 8085 &

sleep 1s
./daft -m machines -c quit

ps

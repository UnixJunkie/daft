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

./daft_mds -m machines & # one MDS

./daft_ds -m machines & # one DS

sleep 1s
./daft -m machines -c quit

ps

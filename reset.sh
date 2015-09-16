
function reset () {
    pkill daft_mds    # kill MDS
    pkill daft_ds     # kill all DSs
    pkill daft        # kill CLI
    \rm -rf /tmp/*.ds # clean previous local datastore
    \rm -f CLI.msg_counter # force to start a new session
}

reset

# to run local tests

pkill daft_mds # kill any MDS

pkill daft_ds # kill any DS

./daft_mds -m machines & # one local MDS in background

rm -rf /tmp/*.ds # clean previous datastore

./daft_ds -r 0 -mds `hostname -f` # one local DS

# to run local tests

pkill daft_mds # kill any MDS

pkill daft_ds # kill any DS

rm -rf /tmp/*.ds # clean previous datastore

./daft_mds -m machines & # one MDS

./daft_ds -r 0 -mds `hostname -f` & # one DS

sleep 1s

echo quit | ./daft_cli

# to run local tests

pkill daft_mds # kill any MDS

pkill daft_ds # kill any DS

rm -rf /tmp/*.ds # clean previous datastore

./daft_mds -m machines & # one MDS

./daft_ds -r 0 -mds `hostname -f` -p 8083 & # one DS
./daft_ds -r 1 -mds `hostname -f` -p 8084 & # one DS
./daft_ds -r 2 -mds `hostname -f` -p 8085 & # one DS

sleep 1s

echo quit | ./daft_cli

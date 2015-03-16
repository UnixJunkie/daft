# to run local tests

pkill dft_mds # kill any MDS

pkill dft_ds # kill any DS

./dft_mds -m machines & # one local MDS in background

rm -rf /tmp/*.ds # clean previous datastore

./dft_ds -r 0 -mds `hostname -f` # one local DS

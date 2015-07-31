all: config build

build:
	obuild build

config:
	obuild configure

clean:
	obuild clean

install:
	ln -sf ${PWD}/dist/build/daft/daft         ${HOME}/bin/
	ln -sf ${PWD}/dist/build/daft_mds/daft_mds ${HOME}/bin/
	ln -sf ${PWD}/dist/build/daft_ds/daft_ds   ${HOME}/bin/

.PHONY: clean dist

all: config build

build:
	obuild build

config:
	obuild configure

clean:
	obuild clean

install:
	ln -sf ${PWD}/bin/daft_ds.sh               ${HOME}/bin/
	ln -sf ${PWD}/bin/daft_mds.sh              ${HOME}/bin/
	ln -sf ${PWD}/bin/daft.sh                  ${HOME}/bin/

dist:
	cd .. && tar cvzf daft.tgz --exclude=\.git daft

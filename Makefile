.PHONY: clean dist

all: config build

build:
	obuild build -j `grep -c processor /proc/cpuinfo`

config:
	obuild configure

clean:
	obuild clean
	\rm -rf cde-package

install:
	ln -sf ${PWD}/bin/daft_ds  ${HOME}/bin/
	ln -sf ${PWD}/bin/daft_mds ${HOME}/bin/
	ln -sf ${PWD}/bin/daft     ${HOME}/bin/

dist:
	cd .. && tar cvzf daft.tgz --exclude=\.git daft

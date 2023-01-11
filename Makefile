.PHONY: build install uninstall reinstall test

build:
        dune build @install -j `getconf _NPROCESSORS_ONLN`

clean:
        rm -rf _build

edit:
        emacs src/*.ml TODO commands.sh &

install: build
        dune install

uninstall:
        dune uninstall

reinstall: uninstall install

dist:
	cd .. && tar cvzf daft.tgz --exclude=\.git daft

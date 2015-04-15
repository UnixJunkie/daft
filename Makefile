all: config build

build:
	obuild build

config:
	obuild configure

clean:
	obuild clean

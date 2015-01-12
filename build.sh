#!/bin/bash

oasis setup
ocaml setup.ml -configure --prefix `opam config var prefix`
ocaml setup.ml -build
#ocaml setup.ml -install

#!/bin/bash

set -x

# source-based installer for ZMQ
if [ ! -f zeromq-4.0.4.tar.gz ] ; then
    wget http://download.zeromq.org/zeromq-4.0.4.tar.gz
fi
tar xzf zeromq-4.0.4.tar.gz
cd zeromq-4.0.4
mkdirhier $HOME/usr/zmq-4.0.4
./configure --prefix=$HOME/usr/zmq-4.0.4 2>&1 > zmq_config.log
(make 2>&1) > zmq_make.log
make install

# # what needs to be appended to LD_LIBRARY_PATH
# echo $HOME/usr/zmq-4.0.4/lib:
# # FBR: add path to lz4's lib

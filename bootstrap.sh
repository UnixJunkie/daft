#!/bin/bash

set -x

# source-based installer for ZMQ
if [ ! -f zeromq-4.0.4.tar.gz ] ; then
    wget http://download.zeromq.org/zeromq-4.0.4.tar.gz
fi
tar xzf zeromq-4.0.4.tar.gz
cd zeromq-4.0.4
mkdir -p $HOME/usr/zmq-4.0.4
./configure --prefix=$HOME/usr/zmq-4.0.4 2>&1 > zmq_config.log
(make 2>&1) > zmq_make.log
make install

# LD_LIBRARY_PATH needs to be updated
echo \~/usr/zmq-4.0.4/lib

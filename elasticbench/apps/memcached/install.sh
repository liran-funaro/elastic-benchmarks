#!/bin/bash
set -e

############################################################################################################
# Install memcached and libmemcached (memslap) on the guest VM
############################################################################################################
# Copy this script to the VM and run it there.


############################################################################################################
# Arguments
############################################################################################################
if [[ $# -lt 1 ]]; then
    echo "Usage: ${0} <path>";
    exit 1;
fi

INSTALL_PATH=$1

############################################################################################################
# Install dependencies
############################################################################################################
sudo apt install build-essential autotools-dev automake libevent-dev mercurial git -y

############################################################################################################
# Get memcached and libmemcached (memslap) source
############################################################################################################
cd ${INSTALL_PATH}

# Clone memcached
git clone https://github.com/liran-funaro/memcached.git memcached-dynamic
git clone https://github.com/memcached/memcached.git

# Get libmemcached
wget https://launchpad.net/libmemcached/1.0/1.0.18/+download/libmemcached-1.0.18.tar.gz
tar xvzf libmemcached-1.0.18.tar.gz

# Clone libmemcached patch (by Liran Funaro)
cd ${INSTALL_PATH}/libmemcached-1.0.18/clients
hg init
echo -e "[paths]\ndefault = https://github.com/liran-funaro/libmemcached-clients.git\n" > .hg/hgrc
hg pull
hg update -C


############################################################################################################
# Compile memcached
############################################################################################################
cd ${INSTALL_PATH}/memcached
./autogen.sh
./configure
make


############################################################################################################
# Compile dynamic memcached
############################################################################################################
cd ${INSTALL_PATH}/memcached-dynamic
./autogen.sh
./configure
make


############################################################################################################
# Compile libmemcached (memslap)
############################################################################################################
cd ${INSTALL_PATH}/libmemcached-1.0.18
# Workaround to fix bug in the configure script cannot find lpthread
# Take from: https://bugs.launchpad.net/libmemcached/+bug/1562677
env LDFLAGS="-L/lib64 -lpthread" ./configure --enable-memaslap

make

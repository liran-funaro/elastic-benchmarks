#!/bin/bash

PROCS=""
for vm in $@; do
    PROCS+=$(cat /sys/fs/cgroup/memory/machine/${vm}.libvirt-qemu/cgroup.procs | xargs);
    PROCS+=" ";
done

htop $(echo $PROCS | sed 's/[0-9]\+/-p &/g')

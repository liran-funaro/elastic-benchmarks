#!/bin/bash

for i in `pgrep kvm`; do
    cat /proc/$i/cmdline|sed -e 's/^.*vm-/vm-/'|sed 's/\.qcow2.*$/ /'
    fgrep VmSwap /proc/$i/status|awk '{print $2}'
    echo
done|fgrep 'vm-'

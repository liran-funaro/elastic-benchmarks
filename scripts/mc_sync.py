#!/usr/bin/env python
"""
Created on Mar 5, 2012

@author: eyal
"""
from subprocess import call
from sys import argv
import time

from cloudexp.util import logs


def mc_sync(n, image):
    logs.start_stdio_logging("info")
    for i in range(1, n + 1):
        time.sleep(i * 0.1)
        cmd = "qemu-img create -b /var/lib/libvirt/images/%s -f qcow2 /var/lib/libvirt/images/vm-%i.qcow2" % (image, i)
        call(cmd, shell = True)


def main():
    n = int(argv[1]) if len(argv) > 1 else 12
    try:
        image = argv[2]
    except:
        image = "moc-master.qcow2"

    mc_sync(n, image)


if __name__ == '__main__':
    main()

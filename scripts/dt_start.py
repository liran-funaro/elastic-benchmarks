#!/usr/bin/env python
'''
Created on Mar 5, 2012

@author: eyal
'''
from subprocess import Popen, PIPE
from sys import argv


def dt_start(n):
    for i in range(1, n + 1):
        curr = Popen(["virsh", "start", "dt-%i" % i], stdout=PIPE, stderr=PIPE)
        err = curr.communicate()[1]
        if len(err.strip()) > 0: print(str(i) + ": " + err.strip())


if __name__ == '__main__':
    n = int(argv[1]) if len(argv) > 1 else 8
    dt_start(n)

#!/usr/bin/env python
'''
Created on Mar 5, 2012

@author: eyal
'''
from subprocess import Popen, PIPE
from sys import argv


def dt_destroy(n):
    for i in range(1, n + 1):
        curr = Popen(["virsh", "destroy", "dt-%i" % i], stdout=PIPE, stderr=PIPE)
        out, err = curr.communicate()
        print(str(i) + ": " + out.strip() + "; " + err.strip())


if __name__ == '__main__':
    n = int(argv[1]) if len(argv) > 1 else 8
    dt_destroy(n)

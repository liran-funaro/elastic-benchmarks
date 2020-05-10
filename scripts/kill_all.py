#!/usr/bin/env python
'''
Created on Mar 5, 2012

@author: eyal
'''
from subprocess import Popen, PIPE
from sys import argv

if __name__ == '__main__':
    machines = argv
    for m in machines:
        if m.startswith("vm"):
            cmd = "pkill -9 python; pkill memcached"
            dest = m
        elif m.startswith("dt"):
            cmd = "pkill -9 python; pkill java"
            dest = "root@" + m
        else:
            continue
        curr = Popen(["ssh", dest] + cmd.split(), stdout=PIPE, stderr=PIPE)
        out, err = curr.communicate()
        print(m + ":")
        print("  out: " + out)
        print("  err: " + err)


#!/usr/bin/env python
"""
Created on Mar 5, 2012

@author: eyal
"""
from subprocess import Popen, PIPE
from sys import argv

if __name__ == '__main__':
    machines = argv[1:]
    for m in machines:
        if m.startswith("vm"):
            cmd = r"ps ax | grep 'python\|memcached'"
            dest = m
        elif m.startswith("dt"):
            cmd = r"ps ax | grep 'python\|start'"
            dest = "root@" + m
        else:
            continue

        curr = Popen(["ssh", dest] + cmd.split(), stdout=PIPE, stderr=PIPE)
        out = curr.communicate()[0]
        out = [l for l in out.split("\n") if l.find("ps") == -1 and l.find("grep") == -1 and l.strip() != ""]

        if len(out) > 0:
            print(">> %s:\n%s" % (m, "\n".join(out)))

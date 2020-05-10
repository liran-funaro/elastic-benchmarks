#!/usr/bin/env python
"""
Created on Mar 5, 2012

@author: eyal
"""
from subprocess import call
from threading import Thread
from sys import argv
import time
import os


def hosts_str(i):
    return """
    127.0.0.1        localhost
    127.0.1.1        howler
    #9.12.168.158    ginkgo-059.cloud9.ibm.com
    192.168.123.""" + str(101 + i) + """   daytrader
    192.168.123.1    ds-had4
    
    # The following lines are desirable for IPv6 capable hosts
    ::1     localhost ip6-localhost ip6-loopback
    fe00::0 ip6-localnet
    ff00::0 ip6-mcastprefix
    ff02::1 ip6-allnodes
    ff02::2 ip6-allrouters
    ff02::3 ip6-allhosts
    """


host_wakeup_time = 40
wakeup_time = 110
short_time = 5


def sync(i):
    call("virsh start dt-%i" % i, shell=True)
    time.sleep(wakeup_time)
    call("ssh root@dt-%i \"echo '%s' > /etc/hosts\"" % (i, hosts_str(i)), shell=True)
    time.sleep(short_time)
    call("virsh destroy dt-%i" % i, shell=True)


def dt_sync(n):
    print("starting master")
    call("virsh start dt-master", shell=True)
    while input(">> enter y to sync master ").strip() == "y":
        call("rsync -azuv %s/moc root@dt-master:/root/workspace" % os.path.expanduser("~"), shell=True)

    if input(">> enter y to destroy master ").strip() == "y":
        call("virsh destroy dt-master", shell=True)

    while input(">> enter y to copy images ").strip() == "y":
        for i in range(1, n + 1):
            time.sleep(i * 0.2)
            call(
                "sudo qemu-img create -b /var/lib/libvirt/images/dt-master.qcow2 -f qcow2 /var/lib/libvirt/images/dt-%i.qcow2" % i,
                shell=True)

    while input(">> enter y to sync vms ").strip() == "y":
        trds = []
        for i in range(1, n + 1):
            time.sleep(i * 0.5)
            trds.append(Thread(target=sync, args=[i]))
            trds[-1].start()

        for trd in trds:
            trd.join()


def main():
    n = int(argv[1]) if len(argv) > 1 else 8
    dt_sync(n)


if __name__ == '__main__':
    main()


# def sync(i):
#    cmd = "rsync -azuv --whole-file %s/moc root@dt-%i:/root/workspace" % (os.path.expanduser("~"), i)
##    cmd = "sudo qemu-img create -b /var/lib/libvirt/images/dt-master.qcow2 -f qcow2 /var/lib/libvirt/images/dt-%i.qcow2" % i
#    call(cmd, shell=True)
#
#
# def dt_sync(n):
#
##    Popen(["virsh start dt-master"], shell=True)
##    time.sleep(15)
##    print "\n====\n".join(Popen(("-azuv --append-verify %s/moc root@dt-master:/root/workspace" % os.path.expanduser("~")).split(),
##                 executable="rsync", stdout=PIPE, stderr=PIPE, shell=True).communicate())
##    time.sleep(5)
##    Popen(["virsh destroy mc-master"], shell=True)
##    time.sleep(2)
#
#
#    trds = []
#    for i in range(1, n + 1):
#        trds.append(Thread(target=sync, args=[i]))
#        trds[-1].start()
#
#    for trd in trds:
#        trd.join()
#
# if __name__ == '__main__':
#    n = int(argv[1]) if len(argv) > 1 else 8
#    dt_sync(n)

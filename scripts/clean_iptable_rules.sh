#!/bin/bash
/sbin/iptables -t nat -D PREROUTING -p tcp -d 132.68.206.92 --dport 11211 -j DNAT --to-destination 192.168.123.2:11211
/sbin/iptables -t nat -D POSTROUTING -p tcp -s 192.168.123.2 --sport 11211 -j SNAT --to-source 132.68.206.92:11211
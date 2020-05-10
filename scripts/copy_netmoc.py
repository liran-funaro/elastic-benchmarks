#!/usr/bin/env python
from subprocess import call

cmd1 = 'cp -Rf ~/moc-network /tmp'
cmd2 = 'sudo cp -Rf /tmp/moc-network /root/'

call(cmd1, shell=True)
call(cmd2, shell=True)

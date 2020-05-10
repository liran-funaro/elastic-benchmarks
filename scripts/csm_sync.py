#!/usr/bin/env python
'''
Created on Mar 5, 2012

@author: eyal
'''
from subprocess import call

if __name__ == '__main__':
    cmd = "rsync -avzW --delete --exclude '*.pyc' --exclude '*.pyo' /home/eyal/moc dl8@ds-out0.cs.technion.ac.il:/home/dl8/workspace"
    call(cmd, shell=True)

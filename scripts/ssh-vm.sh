#!/bin/bash

ssh root@$1 -o ConnectionAttempts=10 -o ConnectTimeout=1

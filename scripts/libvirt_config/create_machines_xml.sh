#!/usr/bin/env bash
################################################################################
# Constants
################################################################################
BASIC_VM_COUNT=24

################################################################################
# Prepare vms XML, define them and add them to known hosts
################################################################################
VM_XML_DIR=vmsxml
mkdir -p ${VM_XML_DIR}

for i in $(seq 1 ${BASIC_VM_COUNT}); do
    mkmachine.pl ${i} > ${VM_XML_DIR}/vm-${i}.xml;
    virsh define vm-${i}.xml;
done

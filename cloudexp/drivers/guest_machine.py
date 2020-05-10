"""
Author: Liran Funaro <liran.funaro@gmail.com>
Original Author: Eyal

Copyright (C) 2006-2019 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import re
import time
from typing import Callable

from cloudexp.util import shell, remote
from cloudexp.util.timeformat import time_delta
from cloudexp.util.decorators import dict_represented
from cloudexp.drivers.libvirt_driver import LibvirtDriver

from mom.logged_object import LoggedObject
from mom.util.terminable import Terminable


class GuestMachineException(Exception):
    pass


@dict_represented
class GuestMachine(LoggedObject, Terminable):
    IMAGES_FOLDER = "/var/lib/libvirt/images"
    SYSCTRL_REGEXP = re.compile(r'([^\s=]+)\s*=\s*([^\s=]+)', re.I | re.MULTILINE)

    def __init__(self, vm_name, libvirt_uri, username, password=None, master_image=None,
                 title=None, swappiness=60, use_image_cow=True,
                 base_mem=2**10, max_mem=2**12, base_vcpus=1, max_vcpus=1, cpu_pin=None,
                 shared_terminable: Terminable = None, **_unknown):
        """
        vm_name: the virtual machine name/alias
        libvirt_uri: a libvirt URI
        username: ssh login username
        password: ssh login password
        master_image: the filename of the master image to use for this machine (should reside on IMAGES_FOLDER)
        title: a title for the machine for documentation purpose
        cpu_pin: a list of cpus to pin this machine to
        swappiness: the swappiness parameter for the guest OS in the VM
        use_image_cow: use copy-on-write mechanism - instead of full copy of the master image
        max_vcpus: set the maximum cpus that is allocated to this machine (will change permanently)
        _unknown: other properties
        """
        LoggedObject.__init__(self, vm_name)
        Terminable.__init__(self, shared_terminable=shared_terminable)

        if master_image is None:
            master_image = "generic-master.qcow2"

        self.vm_name = vm_name
        self.libvirt_uri = libvirt_uri
        self.username = username
        self.password = password
        self.master_image = master_image
        self.title = title
        self.swappiness = swappiness if 0 <= swappiness <= 100 else 60
        self.use_image_cow = use_image_cow

        self.base_mem = base_mem
        self.max_mem = max_mem

        self.base_vcpus = base_vcpus
        self.max_vcpus = max_vcpus
        self.cpu_pin = cpu_pin

        self.driver = LibvirtDriver(self.libvirt_uri)
        self.dom_driver = self.driver.get_domain(self.vm_name)

        self.mac = self.dom_driver.mac()
        self.ip = self.dom_driver.ip()

        self.logger.info("IP: %s", str(self.ip))

    def as_dict(self):
        fields = ('vm_name', 'master_image', 'title', 'ip', 'mac', 'base_mem', 'max_mem',
                  'base_vcpus', 'max_vcpus', 'cpu_pin', 'swappiness', 'use_image_cow')
        return {f: getattr(self, f) for f in fields}

    def get_xml_desc(self):
        return self.dom_driver.descriptor()

    def set_max_vcpus(self, max_vcpus):
        self.max_vcpus = max_vcpus
        self.dom_driver.update_max_vcpus(max_vcpus)

    def set_cpu_count(self, cpu_count):
        self.dom_driver.set_cpu_count(cpu_count)
        self.logger.info("CPU count set to %i", cpu_count)

    def set_max_mem(self, max_mem_mb):
        self.max_mem = max_mem_mb
        self.dom_driver.set_max_memory(max_mem_mb << 10)

    def set_mem(self, target_mb):
        if self.dom_driver.is_active():
            self.dom_driver.set_memory(target_mb << 10)
            self.logger.info("Memory set to %i MB", target_mb)

    def get_cpu_count(self):
        return self.dom_driver.info()['vcpus_count']

    def get_required_cpu_count(self):
        return self.max_vcpus

    def set_cpu_pin(self, cpu_pin=None):
        if not cpu_pin:
            return

        if type(cpu_pin) not in (list, tuple):
            cpu_pin = [cpu_pin]

        self.cpu_pin = cpu_pin
        if self.dom_driver.is_active():
            self.dom_driver.set_pinned_vcpus(cpu_pin)
            self.logger.info("CPU pin set to: %s", cpu_pin)

    def reset_image(self):
        self.dom_driver.reset_image(self.master_image, enable_image_cow=self.use_image_cow)

    def start_domain(self, restart_if_activated=False):
        # if domain active- destroy it
        if self.dom_driver.is_active():
            if not restart_if_activated:
                return
            self.logger.warning("Machine was activated. Restarting machine.")
            self.dom_driver.destroy()

        self.reset_image()

        self.set_max_mem(self.max_mem)
        self.set_max_vcpus(self.max_vcpus)

        # start the domain
        self.dom_driver.start()

        self.set_mem(self.base_mem)
        self.set_cpu_count(self.base_vcpus)
        self.set_cpu_pin(self.cpu_pin)

    def destroy_domain(self):
        try:
            self.dom_driver.destroy()
            self.logger.info("Destroyed")
        except Exception as e:
            self.logger.exception("Couldn't destroy VM: %s", e)

    def ssh(self, cmd, *cmd_args, is_python=False, is_module=True, cwd=None, name=None, important=False, **kwargs):
        ssh_cmd = []
        if cwd is not None:
            ssh_cmd.extend(['cd', f'{cwd};'])

        # set nice for remote programs
        if important:
            ssh_cmd.extend(["nice", "-20"])
        if is_python:
            # -m mod : run library module as a script (terminates option list)
            # -O     : remove assert and __debug__-dependent statements; add .opt-1 before
            #          .pyc extension; also PYTHONOPTIMIZE=x
            # -OO    : do -O changes and also discard docstrings; add .opt-2 before
            #          .pyc extension
            ssh_cmd.append("python")
            if important:
                ssh_cmd.append("-OO")
            if is_module:
                ssh_cmd.append('-m')
        if isinstance(cmd, str):
            ssh_cmd.extend(cmd.split())
        elif type(cmd) in (list, tuple):
            ssh_cmd.extend(cmd)
        else:
            raise ValueError("cmd must be a string or a list of strings.")
        ssh_cmd.extend(cmd_args)
        if name is None:
            name = self.vm_name
        else:
            name = f"{self.vm_name}-{name}"
        return shell.SshClient(ssh_cmd, self.ip, self.username, password=self.password, name=name, **kwargs)

    def rsync(self, source, destination, name=None, **kwargs):
        if name is None:
            name = self.vm_name
        else:
            name = f"{self.vm_name}-{name}"
        return shell.RsyncClient(source, destination, self.ip, self.username,
                                 password=self.password, name=name, **kwargs)

    def remote_function_call(self, remote_function: Callable, *args, output_file=None, **kwargs):
        self.logger.debug("Starting remote function call: %s", remote_function.__name__)
        ssh_class = self.ssh(remote.__name__, is_python=True, is_module=True, name=remote_function.__name__,
                             output_file=output_file, encoding=None, important=True)
        remote.remote_process_function_call(ssh_class.proc, remote_function, *args, **kwargs)
        return ssh_class

    def wait_for_ssh_server(self):
        self.logger.info("Waiting for SSH daemon...")

        time_limitation = 60
        ssh_timeout = 1
        count = 0
        max_retries = int(time_limitation / ssh_timeout)

        while self.should_run:
            start = time.time()
            out, err = self.ssh("echo", "1", name="test", timeout=ssh_timeout,
                                verbose=False).communicate(timeout=ssh_timeout+1)
            if out == "1":
                self.logger.info("SSH daemon is alive")
                return
            count += 1
            if count >= max_retries:
                raise GuestMachineException(f"SSH not ready after {time_delta(time_limitation)}.\n"
                                            f"OUT: {out}\n"
                                            f"ERR: {err}")
            self.terminable_sleep(ssh_timeout - (time.time() - start))

    def set_guest_host_name(self, name=None):
        if name is None:
            name = self.vm_name
        self.ssh_retry('hostnamectl', 'set-hostname', name)

    def set_vm_props(self):
        props = (
            'vm.min_free_kbytes=0',
            'vm.overcommit_memory=1',  # prevent OOM error for process
            f'vm.swappiness={self.swappiness}',
            f'kernel.shmmax={2**34}'
            #                 'vm.mmap_min_addr=0',
            #                 'vm.vfs_cache_pressure=0',
        )

        self.ssh_retry("sysctl", *props, name="set-vm-props", verbose=False)

    def log_vm_sysctl_properties(self):
        fields = (
            'kernel.hostname',
            'kernel.shmmax',
            'kernel.shmall',
            'vm.min_free_kbytes',
            'vm.mmap_min_addr',
            'vm.overcommit_memory',
            'vm.swappiness',
            'vm.lowmem_reserve_ratio',
        )
        max_len = max(map(len, fields))

        try:
            out, _err = self.ssh("sysctl", *fields, name="vm-props-get", verbose=False).communicate(timeout=60)
            res = [f"{k:>{max_len + 3}}: {v}" for k, v in self.SYSCTRL_REGEXP.findall(out)]
            self.logger.debug("VM Info:\n%s", "\n".join(res))
        except Exception as e:
            self.logger.critical("Error getting sysctl info, cannot confirm correctness: %s", e)

    def rsync_retry(self, *args, name=None, max_retries=5, time_before_retry=5, **kwargs):
        for i in range(max_retries):
            if not self.should_run:
                return
            out, err = self.rsync(*args, name=name, **kwargs).communicate()
            if err is None or len(err) == 0:
                break

            if i < max_retries - 1:
                self.logger.warning("Could not %s to remote machine: %s. Retrying (%s/%s)...",
                                    name, err, i + 1, max_retries)
                self.terminable_sleep(time_before_retry)
            else:
                raise ConnectionError(f"Failed to sync {name} to remote machine: {err}")

    def ssh_retry(self, *args, name=None, timeout=60, max_retries=5, time_before_retry=5, **kwargs):
        for i in range(max_retries):
            if not self.should_run:
                return
            try:
                out, err = self.ssh(*args, name=name, **kwargs).communicate(timeout=timeout)
            except Exception as e:
                err = e
            if err is None or not isinstance(err, Exception) or len(err) == 0:
                return

            if i < max_retries - 1:
                self.logger.warning("Could not %s on remote machine: %s. Retrying (%s/%s)...", name, err, i + 1,
                                    max_retries)
                self.terminable_sleep(time_before_retry)
            else:
                raise ConnectionError(f"Failed to {name} on remote machine: {err}")

"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
import os
import sys
import time
import pprint
import signal
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from cloudexp.drivers import cpu_settings
from cloudexp.util.timeformat import time_delta
from cloudexp.exp.exp_machine import ExpMachine
from cloudexp.exp.host_machine import HostMachine
from cloudexp.util.decorators import dict_represented
from cloudexp.exp.mom_executor import MomThreadExecutor

from mom.util.terminable import Terminable
from mom.logged_object import LoggedObject
from mom.config import DictConfigType, DictConfig

try:
    import libvirt
except ImportError:
    libvirt = None


DEFAULT_EXP_CONFIG = {
    'experiment': {
        'thread-monitor-interval': 10,
    },
    'guest-credentials': {
        'username': 'root',
        'password': '1234',
    },
}


@dict_represented
class Experiment(LoggedObject, Terminable):
    def __init__(self, vms_desc: dict, duration: int, output_path: str,
                 host_mom_config: DictConfigType = None,
                 exp_config: DictConfigType = None, extra_info: Optional[dict] = None):
        """
        Parameters:
        -----------
        vms_desc : descriptor of the virtual machines
        duration : the experiment duration
        output_path : the experiment output folder path
        host_mom_config : override configurations in default MOM config
        exp_config: override configurations in default experiment config
        extra_info : extra information to write to the info file
        """
        LoggedObject.__init__(self)
        Terminable.__init__(self)
        # Only this experiment will catch sigint and will terminate the experiment
        self.terminate_on_signal(signal.SIGINT)

        self.duration = duration
        self.output_path = output_path
        self.extra_info = extra_info

        cpu_settings.disable_hyper_threading()
        self.host_machine = HostMachine()
        self.host_machine.set_owner_cpu_count('system', 2)

        self.exp_config = DictConfig(DEFAULT_EXP_CONFIG, exp_config)

        self.mom_executor = MomThreadExecutor(host_mom_config, shared_terminable=self)
        self.mom = self.mom_executor.mom
        self.host_config = self.mom.config
        self.threads = []

        # Start the connection to libvirt
        libvirt_uri = self.host_config.get('main', 'libvirt-hypervisor-uri')
        self.conn = libvirt.open(libvirt_uri)

        self.host_name = self.conn.getHostname()
        self.max_vcpus = self.conn.getMaxVcpus(self.conn.getType())

        # Get the machines. This will update the available CPU after pinning the CPUs to the VMs
        self.number_of_guests = 0
        self.vms = self.get_all_vms_instances(vms_desc)

        # save test information file
        try:
            self.write_info(self.output_path)
        except Exception as e:
            self.logger.error("Could not write info file: %s", e)

        self.logger.debug("Python version: %s", sys.version)
        self.logger.debug('Host name: %s', self.host_name)
        self.logger.debug('Max qemu vCPUs: %i', self.max_vcpus)
        self.logger.debug('Allocated CPUs: %s', pprint.pformat(self.host_machine.cpu_owners))
        self.logger.info("Experiment initiated: %s", output_path)

    def keyboard_interrupt_handler(self, signalnum, frame):
        self.logger.warning("Terminating experiment due to keyboard interrupt.")
        Terminable.keyboard_interrupt_handler(self, signalnum, frame)

    def as_dict(self):
        info = dict(
            duration=self.duration,
            vms={vm.vm_name: vm.as_dict() for vm in self.vms},
            output_path=self.output_path,
            host_machine=self.host_machine.as_dict(),
            host_name=self.host_name,
            max_vcpus=self.max_vcpus,
            exp_config=self.exp_config.get_dict(),
            host_config=self.host_config.get_dict(),
        )

        # Add extra_info to info dict (with respect to overlapping keys)
        if self.extra_info:
            for inf_key, inf_val in self.extra_info.items():
                if inf_key not in info:
                    info[inf_key] = inf_val
                elif info[inf_key] != inf_val:
                    info[inf_key + "(extra-info)"] = inf_val

        return info

    def get_output_file_path(self, file_name):
        return os.path.join(self.output_path, file_name)

    def get_machine(self, name, props):
        self.number_of_guests += 1
        return ExpMachine(name, self.output_path, self.exp_config, self.host_config, shared_terminable=self,
                          shared_deferred_start=self.mom_executor, **props)

    def get_all_vms_instances(self, vms_desc: dict):
        ret = []

        for name, props in vms_desc.items():
            ret.append(self.get_machine(name, props))

        for vm in ret:
            if vm.cpu_pin is None:
                vm_cpus = self.host_machine.set_owner_cpu_count(vm.vm_name, vm.get_required_cpu_count())
                vm.set_cpu_pin(vm_cpus)
            else:
                self.host_machine.set_owner_cpus(vm.vm_name, vm.cpu_pin)

        for vm in ret:
            bm_cpus = self.host_machine.set_owner_cpu_count(f"{vm.vm_name}-benchmark", vm.get_benchmark_cpu_count())
            vm.set_benchmark_cpus(bm_cpus)

        return ret

    def parallel_for_each_vm(self, func, thread_name_prefix='experiment-vm-pool'):
        self.logger.info("Starting parallel VM call: %s", thread_name_prefix)
        with ThreadPoolExecutor(thread_name_prefix=thread_name_prefix) as e:
            for vm, res in zip(self.vms, e.map(func, self.vms)):
                self.logger.debug("%s: %s finished successfully: %s", vm.vm_name, thread_name_prefix, res)

    def close(self):
        if self.conn:
            self.conn.close()

        self.host_machine.end_experiment()

    def destroy_vms_experiment(self):
        try:
            self.logger.info("End experiment for each VMs...")
            self.parallel_for_each_vm(lambda vm_m: vm_m.end_experiment(), "end-experiment")
        except Exception as ex:
            self.logger.exception("Error ending VMs: %s", ex)
            self.logger.critical("Experiment failed due to exception: %s", ex)
        finally:
            self.logger.info("Destroying VMs...")
            self.parallel_for_each_vm(lambda vm_m: vm_m.destroy_domain(), "destroy-domain")

    def start_experiment(self):
        begin_time = time.time()
        try:
            begin_time = self.run_everything()
            self.logger.info("Running experiment for duration of %s", time_delta(self.duration))
            self.terminable_sleep(self.duration)
        except Exception as ex:
            self.logger.exception("EXCEPTION: %s", ex)
            self.logger.critical("Experiment failed due to exception: %s", ex)
            self.parallel_for_each_vm(lambda vm_m: vm_m.destroy_domain(), "destroy-domain")
        finally:
            end_time = time.time()
            self.logger.info("Experiment ended after %s", time_delta(end_time - begin_time))
            self.end_everything()

    def write_vms_desc(self):
        for vm in self.vms:
            with open(self.get_output_file_path(f"{vm.vm_name}.xml"), "w") as f:
                f.write(vm.get_xml_desc())

    def destroy_all_active_vms(self):
        self.logger.info("Destroying active VMs...")
        domain_list = self.conn.listDomainsID()
        for guest_id in domain_list:
            dom = self.conn.lookupByID(guest_id)
            dom.destroy()

    def start_experiment_vms(self):
        self.logger.info("Starting VMs...")
        # libvirt needs domain to start serially:
        for vm in self.vms:
            vm.start_domain()

    def run_everything(self):
        cpu_settings.set_cpu_governor('performance')
        self.host_machine.begin_experiment()
        self.destroy_all_active_vms()
        self.start_experiment_vms()
        self.write_vms_desc()

        # all initiations can be done in parallel:
        self.parallel_for_each_vm(lambda vm_m: vm_m.init_experiment(), "init-experiment")

        # Assert the all initiation succeeded
        if not all(vm.is_ready_for_experiment for vm in self.vms):
            raise Exception("VMs are not ready for experiment")

        self.logger.info("Start MOM and wait for VMs. When finish, trigger start event and load workers...")
        self.mom_executor.start_and_wait_for_guests(self.number_of_guests)
        return self.mom_executor.start_time

    def end_everything(self):
        try:
            self.logger.info("Ending MOM")
            self.mom_executor.terminate()
        except Exception as ex:
            self.logger.exception("Error ending MOM: %s", ex)
            self.logger.critical("Experiment failed due to exception: %s", ex)

        self.destroy_vms_experiment()

        try:
            if self.mom_executor.is_alive():
                self.mom_executor.join(30)
        except Exception as ex:
            self.logger.exception("Error waiting for MOM: %s", ex)
            self.logger.critical("Experiment failed due to exception: %s", ex)

        cpu_settings.enable_hyper_threading()
        cpu_settings.set_cpu_governor('powersave')

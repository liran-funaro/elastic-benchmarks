"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

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
import copy
import socket

from cloudexp.drivers import cpu_settings
from cloudexp.util.logs import get_verbosity_level
from cloudexp.util.shell import run, append_to_file
from cloudexp.util.decorators import dict_represented

from mom.logged_object import LoggedObject


@dict_represented
class HostMachine(LoggedObject):
    """ Manage the host machine resources during an experiment. """

    def __init__(self):
        LoggedObject.__init__(self)
        self._cpu_data = cpu_settings.CpuData()
        self._cpu_owners = {}

    def as_dict(self):
        return dict(
            hostname=self.hostname,
            cpu_hierarcy=self._cpu_data.cpu_hierarchy,
            cpu_owners=self.cpu_owners,
        )

    @property
    def hostname(self):
        return socket.gethostname()

    @property
    def cpu_owners(self):
        return copy.deepcopy(self._cpu_owners)

    @property
    def held_cpus(self):
        ret = set()
        for cpu_list in self._cpu_owners.values():
            ret.update(cpu_list)
        return ret

    def get_owner_cpus(self, owner):
        return self._cpu_owners.get(owner, [])

    def release_owner_cpus(self, owner):
        cpu_list = self._cpu_owners.pop(owner, [])
        # Release only CPUs that are not held by any other owner
        released = set(cpu_list) - self.held_cpus
        self._cpu_data.release_cpus(released)
        return released

    def set_owner_cpus(self, owner, cpu_list):
        prev_cpu_list = self.get_owner_cpus(owner)
        if prev_cpu_list == cpu_list:
            return

        self.release_owner_cpus(owner)
        cpu_list = self._cpu_data.select_cpus(cpu_list)
        self._cpu_owners[owner] = cpu_list
        return cpu_list

    def set_owner_cpu_count(self, owner, cpu_count, shared_owner='system'):
        prev_cpu_list = self.get_owner_cpus(owner)
        if len(prev_cpu_list) == cpu_count and cpu_count > 0:
            return

        self.release_owner_cpus(owner)
        if cpu_count <= 0:
            cpu_list = self.get_owner_cpus(shared_owner)
        else:
            cpu_list = self._cpu_data.auto_select_cpus(cpu_count)
        self._cpu_owners[owner] = cpu_list
        return cpu_list

    def write_system_file(self, file_path: str, data, name, err_log_level='error'):
        out, err = append_to_file(file_path, data, as_root=True)
        if err:
            log_level = get_verbosity_level(err_log_level)
            self.logger.log(log_level, "Fail to %s: %s", name, err)
            return False

        self.logger.info("%s was successful.%s", name, (" [stdout: %s]" % out) if out else "")
        return True

    def drop_caches(self):
        sync_out, sync_err = run("sync", as_root=True)

        if sync_err:
            self.logger.error("Fail to sync cache to secondary memory: %s", sync_err)

        drop_out, drop_err = append_to_file("/proc/sys/vm/drop_caches", 3, as_root=True)
        if drop_err:
            self.logger.error("Fail to drop caches: %s", drop_err)

        self.logger.info("Dropped caches.%s%s",
                         (" [sync: %s]" % sync_out) if sync_out else "",
                         (" [drop: %s]" % drop_out) if drop_out else "")

    def disable_ksm(self):
        success = self.write_system_file("/sys/kernel/mm/ksm/run", 0, "Disable KSM")
        err_log_level = 'info' if success else 'error'
        self.write_system_file("/sys/kernel/mm/ksm/merge_across_nodes", 0, "Disable KSM merge across nodes",
                               err_log_level=err_log_level)

    def service_command(self, service, command):
        out, err = run(["service", str(service), str(command)], as_root=True)

        if err:
            self.logger.error(f"Fail to %s service %s: %s", command, service, err)
        else:
            self.logger.info("Successfully %s service %s.%s", command, service, " [stdout: %s]" % out if out else "")

    def disable_cron(self):
        self.service_command('cron', 'stop')

    def enable_cron(self):
        self.service_command('cron', 'start')

    def begin_experiment(self):
        self.disable_ksm()
        self.drop_caches()
        self.disable_cron()

    def end_experiment(self):
        self.enable_cron()
        for owner in list(self._cpu_owners):
            self.release_owner_cpus(owner)

    def disable_hyper_threading(self):
        cpu_settings.disable_hyper_threading()
        self._cpu_data = cpu_settings.CpuData()

    def enable_hyper_threading(self):
        cpu_settings.enable_hyper_threading()
        self._cpu_data = cpu_settings.CpuData()

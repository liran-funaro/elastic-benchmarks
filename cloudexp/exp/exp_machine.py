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
import os
from threading import Event
from typing import Callable, Union

import cloudexp
import cloudexp.guest.application.benchmark
from cloudexp.guest import application
from cloudexp.exp.benchmark_executor import BenchmarkExecutor
from cloudexp.guest.remote_handlers import remote_application, remote_guest_server
from cloudexp.drivers.guest_machine import GuestMachineException, GuestMachine

from mom import momguestd
from mom.config import DictConfig
from mom.logged_object import LoggedThread

from mom.communication.guest_client import load_guest_client
from mom.util.terminable import Terminable, DeferredStartThread


class ExpMachine(GuestMachine):
    def __init__(self, vm_name, output_path, exp_config: DictConfig, host_config: DictConfig,
                 application: application.Application, benchmark: cloudexp.guest.application.benchmark.Benchmark,
                 guest_mom_config: Union[DictConfig, dict, None] = None,
                 shared_terminable: Terminable = None,
                 shared_deferred_start: DeferredStartThread = None, **props):
        libvirt_uri = host_config.get('main', 'libvirt-hypervisor-uri')
        username = exp_config.get('guest-credentials', 'username')
        password = exp_config.get('guest-credentials', 'password')
        GuestMachine.__init__(self, vm_name, libvirt_uri, username, password, application.get_image_name(),
                              shared_terminable=shared_terminable, **props)

        self.output_path = output_path
        self.exp_config = exp_config
        self.host_conf = host_config
        self.guest_config = DictConfig(momguestd.DEFAULT_CONFIG, guest_mom_config)

        self.application = application
        self.benchmark = benchmark

        self.load_func = props.get('load_func', None)
        self.load_interval = props.get('load_interval', None)
        self.expected_load_rounds = props.get("expected_load_rounds", None)

        self.benchmark.set_guest_machine(self)
        self.application.set_guest_server_port(self.host_conf.get("guest-client", "port"))
        self.guest_client = load_guest_client(self.ip, self.vm_name, self.host_conf, base_name='exp-machine-client')
        self.benchmark_thread = BenchmarkExecutor(self.benchmark, self.load_func, self.load_interval,
                                                  self.expected_load_rounds, name=self.vm_name,
                                                  shared_terminable=shared_terminable,
                                                  shared_deferred_start=shared_deferred_start)

        self.threads = {}
        self.ssh_connections = {}
        self._is_ready_for_experiment = Event()

    def as_dict(self):
        return dict(GuestMachine.as_dict(self),
                    load_interval=self.load_interval,
                    bm_cpus=self.get_benchmark_cpus(),
                    guest_config=self.guest_config.get_dict(),
                    )

    def get_benchmark_cpu_count(self):
        return self.benchmark.get_required_cpus()

    def set_benchmark_cpus(self, bm_cpus):
        self.benchmark.set_benchmark_cpus(bm_cpus)

    def get_benchmark_cpus(self):
        return self.benchmark.get_benchmark_cpus()

    def init_experiment(self):
        self.logger.info("Initiating experiment on VM...")

        try:
            self.start_domain(restart_if_activated=False)
            self.wait_for_ssh_server()
            self.set_guest_host_name()
            self.set_vm_props()
            self.log_vm_sysctl_properties()
            self.install_framework()
            self.install_application()
            self.disable_cron()
            self.drop_caches()

            self.start_guest_server()
            self.guest_client.wait_for_server(shared_terminable=self)

            self.start_application()
            self.benchmark.wait_for_application()

            # Start benchmark thread
            self.benchmark_thread.start()
        except Exception as e:
            self.logger.exception("Failed to initiate experiment: %s", e)
            raise e
        else:
            self._is_ready_for_experiment.set()

    @property
    def is_ready_for_experiment(self):
        return self._is_ready_for_experiment.is_set()

    def end_experiment(self):
        self.terminate()
        self.benchmark_thread.terminate()
        self.close_client()
        self._is_ready_for_experiment.clear()

        self.logger.info("Waiting for remote guest server to end...")
        self.join_guest_server(20)
        self.logger.info("Waiting for remote program to end...")
        self.join_application(20)

    def close_client(self):
        if self.guest_client is not None:
            self.guest_client.close()

    def disable_cron(self):
        self.logger.info("Disabling Cron...")
        ssh_cron = self.ssh("service", "cron", "stop", name="disable-cron")

        ssh_cron.communicate()  # short blocking

        if not ssh_cron.err:
            self.logger.info("Cron disabled: %s", ssh_cron.out.replace("\n", " ").strip())
        else:
            self.logger.error("Fail to disable cron: out=%s, err=%s", ssh_cron.out, ssh_cron.err)

    def drop_caches(self):
        ssh_sync = self.ssh("sync", name="sync-cache")
        ssh_sync.communicate()  # short blocking

        if ssh_sync.err:
            self.logger.error("Fail to sync cache to secondary memory: %s", ssh_sync.err)

        ssh_drop = self.ssh("echo 3 > /proc/sys/vm/drop_caches", name="drop-cache")
        if ssh_drop.err:
            self.logger.error("Fail to drop caches: %s", ssh_drop.err)

        self.logger.info("Dropped caches.%s%s",
                         (" [sync: %s]" % ssh_sync.out) if ssh_sync.out else "",
                         (" [drop: %s]" % ssh_drop.out) if ssh_drop.out else "")

    def start_background_thread(self, target, name_prefix=None, args=(), kwargs=None):
        if not self.should_run:
            return

        name = f"{name_prefix}-{self.vm_name}"

        self.logger.info("Starting background thread: %s", name)
        try:
            t = LoggedThread(target=target, name=name, args=args, kwargs=kwargs, daemon=True)
            self.threads[name] = t
            t.start()
        except Exception as e:
            self.logger.exception("Failed to initiated thread '%s': %s", name, e)

    def _remote_function_call_thread(self, name, remote_function: Callable, *args, **kwargs):
        output_file = os.path.join(self.output_path, f'{name}-{self.vm_name}.log')
        conn = self.remote_function_call(remote_function, *args, output_file=output_file, **kwargs)
        self.ssh_connections[name] = conn
        out, err = conn.communicate()  # blocking
        if self.should_run:
            self.logger.error("%s crashed before its time - [out]: %s, [err]: %s", name, out, err)

    def start_remote_function_call_thread(self, name, remote_function: Callable, *args, **kwargs):
        self.start_background_thread(self._remote_function_call_thread, name,
                                     args=(name, remote_function, *args), kwargs=kwargs)

    def start_application(self):
        self.start_remote_function_call_thread("application", remote_application, self.application,
                                               self.guest_config.get('logging', 'verbosity'))

    def start_guest_server(self):
        self.start_remote_function_call_thread("guest-server", remote_guest_server,
                                               self.guest_config.get('logging', 'verbosity'),
                                               self.guest_config, guest_name=self.vm_name)

    def terminate_ssh(self, ssh_name, timeout):
        ssh_thread = self.ssh_connections.get(ssh_name, None)
        if ssh_thread is None:
            return
        if ssh_thread.is_alive():
            ssh_thread.terminate()
            ssh_thread.join(timeout)

    def join_application(self, timeout=None):
        self.terminate_ssh('application', timeout)

    def join_guest_server(self, timeout=None):
        self.terminate_ssh('guest-server', timeout)

    def install_framework(self):
        self.logger.info("Copy and install Python's code...")

        exclude = '*.pyc', '*.pyo', '__pycache__', '.*'
        self.rsync_retry(cloudexp.repository_path, '~', name='framework', exclude=exclude, delete=True)

        args = ['guest_setup.py', 'develop', '--user']
        out, err = self.ssh(*args, cwd=cloudexp.repository_name, is_python=True, is_module=False,
                            name="install-packages").communicate()
        if err:
            raise GuestMachineException(f"Could not install framework:\nOUT: {out}\nERR: {err}")

    def install_application(self):
        bin_path = self.application.get_bin_path()
        if bin_path is None:
            self.logger.info("Application does not require to install binaries...")
            return
        self.logger.info("Copy and install application's binaries...")
        if type(bin_path) not in (list, tuple):
            assert isinstance(bin_path, str), "Binaries path must be a string."
            bin_path = (bin_path,)

        for p in bin_path:
            self.rsync_retry(p, '~', name='app-binaries', delete=True)

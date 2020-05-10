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
import re

from cloudexp.guest.application import Application, DynamicResourceControl, run_app_with_args
from elasticbench.apps.postgresql import USERNAME, DATA_DIR, CONFIG_FILE_PATH, postgres_bin_path, \
    POSTGRES_PORT, STATS_TEMP_DIR
from mom.util.parsers import parameter_int, parameter_bool, parameter_float


##########################################################################################
# PostgresSQL Options:
##########################################################################################
#   -B NBUFFERS     number of shared buffers
#   -c NAME=VALUE   set run-time parameter
#   -d 1-5          debugging level
#   -D DATADIR      database directory
#   -e              use European date input format (DMY)
#   -F              turn fsync off
#   -h HOSTNAME     host name or IP address to listen on
#   -i              enable TCP/IP connections
#   -k DIRECTORY    Unix-domain socket location
#   -l              enable SSL connections
#   -N MAX-CONNECT  maximum number of allowed connections
#   -o OPTIONS      pass "OPTIONS" to each server process (obsolete)
#   -p PORT         port number to listen on
#   -s              show statistics after each query
#   -S WORK-MEM     set amount of memory for sorts (in kB)
#   --NAME=VALUE    set run-time parameter
#   --describe-config  describe configuration parameters, then exit
#   --help          show this help, then exit
#   --version       output version information, then exit
class PostgreSQL(Application):
    def __init__(self,
                 max_connections=None,
                 shared_buffers_mb=None,
                 work_mem_mb=None,
                 temp_buffers_mb=None,
                 auto_vacuum=None,
                 checkpoint_timeout=None,
                 checkpoint_completion_target=None,
                 max_wal_size_mb=None,
                 checkpoint_mem_ratio=0.5,
                 mem_change_threshold=0.05,
                 **kwargs
                 ):
        Application.__init__(self, dynamic=True, stderr_log_level='info', **kwargs)
        self.max_connections = parameter_int(max_connections, 256)
        self.shared_buffers_MB = parameter_int(shared_buffers_mb, 4096)
        self.work_mem_MB = parameter_int(work_mem_mb, 128)
        self.temp_buffers_MB = parameter_int(temp_buffers_mb, 128)
        self.auto_vacuum = parameter_bool(auto_vacuum, False)
        self.checkpoint_timeout = parameter_int(checkpoint_timeout, 900)
        self.checkpoint_completion_target = parameter_float(checkpoint_completion_target, 0.9)
        self.max_wal_size_mb = parameter_int(max_wal_size_mb, None)
        self.checkpoint_mem_ratio = parameter_float(checkpoint_mem_ratio, 0.5)
        self.mem_change_threshold = parameter_float(mem_change_threshold, 0.05)

    @staticmethod
    def get_image_name(*_args, **_kwargs):
        return "generic-master.qcow2"

    def start_application(self):
        # Create stats dir
        os.makedirs(STATS_TEMP_DIR, exist_ok=True)
        run_app_with_args(['chown', f'{USERNAME}.{USERNAME}', STATS_TEMP_DIR, '-R']).communicate()

        # Log postgres version
        postgres_bin = postgres_bin_path('postgres')
        out, _err = run_app_with_args([postgres_bin, '--version']).communicate()
        self.logger.info("PostgreSQL version: %s", out)

        args = [
            "sudo",
            "-u", USERNAME,
            postgres_bin,
            "-p", str(POSTGRES_PORT),  # -p PORT         port number to listen on
            "-D", DATA_DIR,            # -D DATADIR: database directory
            '-c', f'config_file={CONFIG_FILE_PATH}',
        ]

        if self.max_connections is not None:
            args += ["-N", str(self.max_connections)]  # -N MAX-CONNECT  maximum number of allowed connections
        if self.work_mem_MB is not None:               # -c NAME=VALUE   set run-time parameter
            args += ["-c", f"work_mem={self.work_mem_MB}MB"]
        if self.shared_buffers_MB is not None:
            args += ["-c", f"shared_buffers={self.shared_buffers_MB}MB"]
        if self.temp_buffers_MB is not None:
            args += ["-c", f"temp_buffers={self.temp_buffers_MB}MB"]
        if self.auto_vacuum is not None:
            args += ["-c", f"autovacuum={'on' if self.auto_vacuum else 'off'}"]
        if self.checkpoint_timeout is not None:
            args += ["-c", f"checkpoint_timeout={self.checkpoint_timeout}s"]
        if self.checkpoint_completion_target is not None:
            args += ["-c", f"checkpoint_completion_target={self.checkpoint_completion_target}"]
        if self.max_wal_size_mb is not None:
            args += ["-c", f"max_wal_size={self.max_wal_size_mb}MB"]

        return run_app_with_args(args)

    def start_resource_control(self):
        """ Start resource control thread """
        return PostgresSQLMemCtrl(self.guest_server_port, self.checkpoint_mem_ratio,
                                  self.mem_change_threshold,
                                  update_settings_func=self.update_settings,
                                  decrease_mem_time=self.decrease_mem_time,
                                  wait_timeout=self.wait_timeout, shared_terminable=self)

    def terminate_application(self):
        self.pg_ctl("stop")

    def pg_ctl(self, op):
        pg_ctl_bin = postgres_bin_path('pg_ctl')
        args = [
            "sudo",
            "-u", USERNAME,
            pg_ctl_bin,
            '-D', DATA_DIR,
            op
        ]
        out, err = run_app_with_args(args).communicate()
        out = out.strip()
        err = err.strip()
        if err:
            self.logger.error("Error in pg_ctrl: %s", err)
        if out:
            self.logger.info("pg_ctl output: %s", out)

    def update_settings(self, **settings):
        self.change_configuration_file(**settings)
        self.pg_ctl("reload")

    @staticmethod
    def change_configuration_file(**settings):
        with open(CONFIG_FILE_PATH, 'r') as f:
            conf = f.read()

        for key, value in settings.items():
            conf_pattern = re.compile(rf'^\s*#?({key}\s*[=\s]\s*)([^\n#]+)(\s*#.*)?$', re.I | re.M)
            conf = conf_pattern.sub(rf'\g<1>{value}\g<3>', conf)

        with open(CONFIG_FILE_PATH, 'w') as f:
            f.write(conf)


class PostgresSQLMemCtrl(DynamicResourceControl):
    GB_IN_MB = 1 << 10

    def __init__(self, guest_server_port, checkpoint_mem_ratio, mem_change_threshold,
                 update_settings_func,
                 wait_timeout=60, decrease_mem_time=1, **kwargs):
        DynamicResourceControl.__init__(self, guest_server_port, wait_timeout, decrease_mem_time, **kwargs)

        self.checkpoint_mem_ratio = checkpoint_mem_ratio
        self.mem_change_threshold = mem_change_threshold
        self.update_settings_func = update_settings_func

        self.change_ratio_threshold_high = 1.0 + self.mem_change_threshold
        self.change_ratio_threshold_low = 1.0 - self.mem_change_threshold

        self.change_high_threshold = 0
        self.change_low_threshold = 0

    @staticmethod
    def calc_effective_cache_size_function(mem_total, mem_usage, mem_cache_and_buff):
        """
        According to PostgresSQL recommendations:
        https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server
        """
        return mem_cache_and_buff + (mem_total - mem_usage)

    def max_wal_size_function(self, mem):
        # According to experiments
        return max(1, int((mem * self.checkpoint_mem_ratio)))

    def shared_buffers_function(self, mem):
        # According to PostgresSQL recommendations
        return max(1, int(mem / 4 if mem > self.GB_IN_MB else int(mem * 0.25)))

    def change_mem_func(self, mem_total, mem_usage, mem_cache_and_buff, app_rss):
        if self.change_low_threshold < mem_total < self.change_high_threshold:
            return

        self.change_high_threshold = mem_total * self.change_ratio_threshold_high
        self.change_low_threshold = mem_total * self.change_ratio_threshold_low

        effective_cache_size = self.calc_effective_cache_size_function(mem_total, mem_usage, mem_cache_and_buff)

        self.update_settings_func(
            # change requires restart
            # shared_buffers=self.shared_buffers_function(mem_total),
            max_wal_size=f'{self.max_wal_size_function(mem_total)}MB',
            effective_cache_size=f'{int(effective_cache_size)}MB'
        )

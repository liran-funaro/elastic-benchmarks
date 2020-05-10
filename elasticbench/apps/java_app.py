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
from typing import Union, Iterable, Optional

from cloudexp.guest.application import Application, DynamicResourceControl, run_app_with_args
from elasticbench import settings

PAGES_IN_MB = 2**8


class JavaCmdError(Exception):
    pass


def get_java_guest_bin(*bin_name):
    return os.path.join(os.path.expanduser("~"), 'j2sdk-image', 'bin', *bin_name)


def get_java_cmd_args(max_heap_size_mb: Union[int, float], *args):
    return (
        get_java_guest_bin('java'),
        f'-Xmx{max_heap_size_mb}m',
        '-XX:+UseAdaptiveSizePolicy',
        '-XX:+UseParallelGC',
        '-XX:-UseParallelOldGC',
        '-XX:+UseAdaptiveSizePolicyWithSystemGC',
        *args
    )


class JavaApplication(Application):
    def __init__(self, *args, max_heap_size_mb=2**10, **kwargs):
        Application.__init__(self, *args, **kwargs)
        self.max_heap_size_mb = max_heap_size_mb

    def start_application(self):
        pass

    def start_resource_control(self):
        pass

    def terminate_application(self):
        pass

    def get_bin_path(self):
        java_path = os.path.join(settings.read_applications_bin(), 'j2sdk-image')
        java_app_path = self.get_java_app_bin_path()
        if java_app_path is None:
            return java_path
        elif type(java_app_path) in (list, tuple):
            return (java_path, *java_app_path)
        else:
            return java_path, java_app_path

    @staticmethod
    def get_java_app_bin_path() -> Union[None, str, Iterable[str]]:
        """ Should return java application bin path in the host """
        return None


class JavaDynamicResourceControl(DynamicResourceControl):
    def __init__(self, java_app: JavaApplication, *args, spare_mem=None, **kwargs):
        self.java_app = java_app
        self.spare_mem = spare_mem
        DynamicResourceControl.__init__(self, *args, **kwargs)

    def gc(self):
        """ Runs System.GC() using jcmd, a tool introduced in OpenJDK 7. """
        if not self.is_alive:
            return

        pid = self.java_app.pid
        jcmd = get_java_guest_bin('jcmd')
        out, err = run_app_with_args([jcmd, str(pid), 'GC.run']).communicate()
        out = out.strip('Parallel Scavenge: using ballooning').strip().strip(f'{pid}:').strip()
        err = err.strip()
        if err or out:
            raise JavaCmdError(f"Failed starting GC for process '{pid}':%s%s." % (
                f' [stderr: {err}]' if err else '', f' [stdout: {out}]' if out else ''))

    def set_balloon_size(self, balloon_size_mb: Union[int, float]):
        """ Writes the size of the balloon to the balloon pipe """
        if not self.is_alive:
            return

        balloon_size_bytes = int(balloon_size_mb * (2**20))
        balloon_pipe = f'/tmp/jvm-balloon-size-bytes-{self.java_app.pid}'

        with open(balloon_pipe, 'w+') as f:
            f.write(f'{balloon_size_bytes}\n')
        # force (hopefully full) gc so that the old gen ballooning can take effect immediately.
        # Otherwise we may have to wait long time.
        self.gc()

    def set_java_heap_size(self, target_heap_size_mb: Union[int, float]):
        balloon_size_mb = self.java_app.max_heap_size_mb - target_heap_size_mb
        if balloon_size_mb < 0:
            self.logger.warning("Target heap size (%s MB) cannot be grater than the maximal heap size (%s MB).",
                                target_heap_size_mb, self.java_app.max_heap_size_mb)
            balloon_size_mb = 0
        self.set_balloon_size(balloon_size_mb)

    def get_heap_size(self) -> Optional[int]:
        """ Return the java process heap size in MB """
        if not self.is_alive:
            return

        try:
            with open(f"/proc/{self.java_app.pid}/statm", "r") as f:
                info = f.read()
            return int(info.split(" ")[1]) / PAGES_IN_MB
        except Exception as err:
            self.logger.warn("Error reading java statm: %s", err)
            return None

    def application_rss(self):
        return self.get_heap_size()

    def change_mem_func(self, mem_total, mem_usage, mem_cache_and_buff, app_rss):
        """ NOTE: Use self.set_java_heap_size(heap_size_mb) to set java heap size. """
        spare = mem_usage - mem_cache_and_buff - app_rss + self.spare_mem
        if spare < 0:
            self.logger.error("Spare memory should be grater than 0. But spare=%s", spare)
        target = mem_total - spare
        self.set_java_heap_size(target)

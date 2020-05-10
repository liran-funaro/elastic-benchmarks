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
import logging
import multiprocessing
import re
import os
import subprocess
import sys
import errno
import signal
from typing import Optional, Union, IO, Set

UMASK = 0

# Default working directory for the daemon.
WORKDIR = "/"

# Default maximum for the number of available file descriptors.
MAXFD = 1024

# The standard I/O file descriptors are redirected to /dev/null by default.
try:
    REDIRECT_TO = os.devnull
except AttributeError:
    REDIRECT_TO = "/dev/null"


TGID_REGEXP = re.compile(r"^tgid[ \t\f\v]*:[ \t\f\v]*(\d+)\s*$", re.MULTILINE | re.IGNORECASE)

SIGNALS = {
    'int': signal.SIGINT,
    'kill': signal.SIGKILL,
}


def get_signal(sig: Union[str, int] = signal.SIGTERM, default: int = signal.SIGTERM):
    if isinstance(sig, str):
        sig = SIGNALS.get(sig, default)
    return sig


class NullDevice:
    def write(self, s):
        pass


def pid_of(process_name):
    return int(subprocess.check_output(["pidof", process_name]).strip())


def kill_process(pid, sig: Union[str, int] = signal.SIGTERM):
    """
    Send a signal to a process
    :param pid: The process PID
    :param sig: The signal (default=SIGTERM)
    :return: True if successful
    """
    sig = get_signal(sig, signal.SIGTERM)
    try:
        os.kill(pid, sig)
        os.waitpid(pid, 0)
    except OSError:
        return False
    else:
        return True


def kill_multiple_process(pids, sig: Union[str, int] = signal.SIGTERM):
    """
    Send a signal to a list of process.
    --Iterating over os.kill() doesn't work well.--
    :param pids: A list of process PIDs
    :param sig: The signal (default=SIGTERM)
    :return: None
    """
    # cmd = ["kill", "-%s" % int(sig), *map(str, pids)]
    # print(" ".join(cmd))
    # subprocess.run(cmd)
    sig = get_signal(sig, signal.SIGTERM)
    group_pids = set()
    for p in pids:
        with open(f"/proc/{p}/status", "r") as f:
            data = f.read()

        m = TGID_REGEXP.search(data)
        if m is None:
            tgid = p
        else:
            tgid = m.group(1)
        group_pids.add(int(tgid))

    for p in group_pids:
        kill_process(p, sig)


def pid_exists(pid):
    """
    Check whether pid exists in the current process table.
    UNIX only.
    Taken from: http://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid
    """
    if not isinstance(pid, int) or pid < 0:
        raise ValueError('invalid PID: must be positive integer')
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')

    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


def write_pid_to_files(*pid_files, logger=None):
    process_pid = multiprocessing.current_process().pid
    for pid_file in pid_files:
        with open(pid_file, 'w') as f:
            f.write(str(process_pid))
    if logger is not None:
        logger.info("Process pid (%s) is written to file(s).", process_pid)


def read_pid_file(pid_file_path: str) -> (Optional[int], Optional[bool]):
    """
    Returns a tuple: pid from a file, is alive (True, False)
    If failed to read the file, return None,None
    """
    try:
        with open(pid_file_path, 'r') as f:
            pid = int(f.read())
    except:
        return None, None

    try:
        os.waitpid(pid, os.WNOHANG)
    except:
        pass

    try:
        import psutil
        return pid, psutil.pid_exists(pid)
    except:
        return pid, False


def find_active_form_pid_files(*pid_file_paths: str) -> Set[int]:
    ret = set()
    for file_path in pid_file_paths:
        pid, alive = read_pid_file(file_path)
        if alive:
            ret.add(pid)
    return ret


def terminate_from_pid_file(pid_file_path, sig: Union[str, int] = signal.SIGINT):
    sig = get_signal(sig, signal.SIGINT)
    try:
        with open(pid_file_path, 'r') as f:
            pid = int(f.read())
    except:
        return False

    return kill_process(pid, sig)


def convert_to_daemon():
    """
    Convert current process to a background daemon.
    Credit goad to Chad J. Schroeder (Copyright (C) 2005 Chad J. Schroeder)
    :return: None. Will exit if fail.
    """
    __fork_to_child()

    os.chdir(WORKDIR)
    os.setsid()
    os.umask(UMASK)

    __fork_to_child()
    __close_all_file_descriptors()

    sys.stdout = StreamToLogger('stdout', logging.INFO)
    sys.stderr = StreamToLogger('stderr', logging.ERROR)


#########################################################################
# Module helper functions
#########################################################################

def __fork_to_child():
    """
    Fork the current process and continue running only in child process
    :return: None. Will exit of fail.
    """
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print("Daemon fork failed: %d (%s)" % (e.errno, e.strerror), file=sys.stderr)
        sys.exit(1)


def __close_all_file_descriptors():
    """
    Closes all the file descriptors of the process and redirect
    stderr and stdout to a fake device.
    stdin will be read from /dev/null.
    :return: None. Will exit if fail.
    """
    try:
        import resource
        maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if maxfd == resource.RLIM_INFINITY:
            maxfd = MAXFD
    except:
        maxfd = MAXFD

    # Iterate through and close all file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:  # ERROR, fd wasn't open to begin with (ignored)
            pass

    sys.stdin = open(REDIRECT_TO, 'r')
    sys.stderr = NullDevice()
    sys.stdout = NullDevice()


class StreamToLogger(IO):
    """
    Taken from:
    https://github.com/jdloft/multiprocess-logging/blob/master/main.py
    """

    def __init__(self, name, log_level=logging.INFO):
        self._name = name
        self.logger = logging.getLogger(name)
        self.logger.level = log_level
        self.buffer_list = []

    @property
    def name(self):
        return self._name

    def write(self, buf):
        """
        Append the buffer to the buffer list
        and attempt to flush a full line
        :param buf: The buffer to write
        :return: None
        """
        self.buffer_list.append(buf)
        self.flush()
        return len(buf)

    def flush(self):
        """
        Will flush the buffer line by line.
        Will not flush "half" lines. Will save for later
        :return: None
        """
        lines = "".join(self.buffer_list).lstrip().split("\n")
        self.buffer_list = lines[-1:]
        lines = lines[:-1]

        for line in lines:
            self.logger.log(self.logger.level, line.rstrip().lstrip())
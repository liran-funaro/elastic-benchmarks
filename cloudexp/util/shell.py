"""
Author: Liran Funaro <liran.funaro@gmail.com>
@author: eyal

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
import itertools
import os
import shlex
import subprocess
from typing import Union, TextIO

from mom.logged_object import LoggedObject


###########################################################################
# Helper Functions
###########################################################################

def normalize_path(path, real_path=False):
    """
    :param path: A path
    :param real_path: If True, will extract the real full path
    :return: A normalize path
    """
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    path = os.path.normcase(path)
    if real_path:
        path = os.path.realpath(path)
    path = os.path.normpath(path)
    return path


def make_sendable_command_string(obj):
    """
    Make a sendable and executable command.
    """
    s = str(obj)
    for c in ["'", '"', "(", ")", "<", ">"]:
        s = s.replace(c, "\\" + c)
    return s


def get_command_line_args(cmd, make_sendable=False):
    """
    Create a command line args list
    """
    try:
        if len(cmd) == 1:
            cmd = cmd[0]
    except:
        pass

    # convert cmd to a list of arguments
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)

    if make_sendable:
        map_func = make_sendable_command_string
    else:
        map_func = str

    return list(map(map_func, cmd))


def get_command_line_string(cmd):
    if isinstance(cmd, str):
        return cmd
    elif isinstance(cmd, list):
        return " ".join(map(str, cmd))
    else:
        raise ValueError("Command line was not a string or a list: %s" % cmd)


def shell_command_line(cmd, as_root=False, nice=None, make_sendable=False, is_local=True):
    cmd_args = list(get_command_line_args(cmd, make_sendable))
    if is_local and len(cmd_args) > 0 and isinstance(cmd_args[0], str):
        cmd_args[0] = normalize_path(cmd_args[0])

    if as_root:
        cmd_args = ["sudo", "-k", "-n", *cmd_args]

    if nice is not None:
        cmd_args = ["nice", str(nice)] + cmd_args

    cmd_str = get_command_line_string(cmd_args)

    return cmd_str, cmd_args


###########################################################################
# Run Functions
###########################################################################

def run(cmd, as_root=False, nice=None, timeout=None, cmd_input=None, **kwargs):
    cmd_str, cmd_args = shell_command_line(cmd, as_root=as_root,
                                           nice=nice, make_sendable=False)

    kwargs["stdin"] = subprocess.PIPE
    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.PIPE

    proc = subprocess.Popen(cmd_args, **kwargs)
    if cmd_input and isinstance(cmd_input, str):
        proc.stdin.write(cmd_input)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()

    out = out.strip().decode("utf-8")
    err = err.strip().decode("utf-8")

    return out, err


SHELL_CMD_ARGS = ["sh", "-c"]
SSH_CMD_ARGS = ['ssh', '-o', 'StrictHostKeyChecking=no']
APPEND_CMD = "echo '%s' > %s"


def run_shell(cmd, as_root=False, nice=None, **kwarg):
    cmd_str, cmd_args = shell_command_line(cmd, as_root=as_root,
                                           make_sendable=False)
    shell_cmd = SHELL_CMD_ARGS + [cmd_str]
    if nice is not None:
        shell_cmd = ["nice", str(nice)] + shell_cmd
    return run(shell_cmd, as_root=as_root, **kwarg)


def run_with_cpus(cmd, cpus=None, as_root=False, **kwarg):
    prog_cmd_args = get_command_line_args(cmd)

    if cpus is not None:
        if type(cpus) not in [list, tuple]:
            cpus = [cpus]

        if len(cpus) > 0:
            cpu_bit_vector = 0
            for cpu in cpus:
                cpu_bit_vector |= (1 << int(cpu))

            prog_cmd_args = ["taskset", "-a", hex(cpu_bit_vector), *prog_cmd_args]

    return run(prog_cmd_args, as_root=as_root, **kwarg)


def append_to_file(file_name, data, as_root=False, **kwarg):
    data = make_sendable_command_string(data)
    append_cmd = APPEND_CMD % (data, file_name)

    return run_shell(append_cmd, as_root=as_root, **kwarg)


def run_on_server(cmd, server, username=None, identity_file=None, timeout=None,
                  as_root=False, nice=None, **kwarg):
    cmd_str, cmd_args = shell_command_line(cmd, as_root=as_root, nice=nice,
                                           make_sendable=False)
    ssh_cmd_args = SSH_CMD_ARGS.copy()

    if identity_file:
        ssh_cmd_args += ['-i', str(identity_file)]

    if timeout:
        ssh_cmd_args += ["-o", f"ConnectTimeout={timeout}"]

    if username is None:
        ssh_cmd_args.append(server)
    else:
        ssh_cmd_args.append(f"{username}@{server}")

    ssh_cmd_args.append(cmd_str)

    return run(ssh_cmd_args, as_root=False, **kwarg)


class RunProcess(LoggedObject):
    def __init__(self, cmd_args, encoding='utf-8', output_file=None, name=None, verbose=True):
        LoggedObject.__init__(self, name)
        self.cmd_args = cmd_args
        self.encoding = encoding
        self.output_file = output_file
        self.verbose = verbose

        self.out = None
        self.err = None

        self.output_stream: Union[TextIO, int] = subprocess.PIPE
        if self.output_file:
            self.output_stream = open(self.output_file, "a")

        if self.verbose:
            self.log_start()
        self.proc = subprocess.Popen(self.cmd_args, encoding=self.encoding, stdin=subprocess.PIPE,
                                     stdout=self.output_stream, stderr=self.output_stream)

    def log_start(self):
        self.logger.debug("Running process: %s", self.cmd_args)

    def log_end(self):
        self.logger.debug("Process ended")

    ##############################################################################################################
    # Mimics some of threading.Thread interface
    ##############################################################################################################

    @property
    def name(self):
        return self.__log_name__

    def join(self, timeout=None):
        if self.proc is None:
            return

        self.proc.wait(timeout)

        if self.output_stream != subprocess.PIPE:
            self.output_stream.close()

        self.log_end()

    def is_alive(self):
        if self.proc is None:
            return False

        return self.proc.poll() is None

    ##############################################################################################################
    # Mimics subprocess.Popen interface
    ##############################################################################################################

    def poll(self):
        if self.proc is None:
            return 1
        return self.proc.poll()

    def wait(self, timeout=None):
        self.join(timeout)

    def communicate(self, timeout=None, stdin=None):
        if self.out is not None:
            return self.out, self.err

        if self.proc is None:
            return

        try:
            out, err = self.proc.communicate(timeout=timeout, input=stdin)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, err = self.proc.communicate()

        if out:
            out = out.strip()
        if err:
            err = err.strip()
        self.out, self.err = out, err
        return out, err

    def send_signal(self, signal):
        if self.proc:
            self.proc.send_signal(signal)

    def terminate(self):
        self.kill()

    def kill(self):
        if self.proc:
            self.proc.kill()

    @property
    def stdin(self):
        if self.proc:
            return self.proc.stdin

    @property
    def stdout(self):
        if self.proc:
            return self.proc.stdout

    @property
    def stderr(self):
        if self.proc:
            return self.proc.stderr

    @property
    def args(self):
        if self.proc:
            return self.proc.args

    @property
    def pid(self):
        if self.proc:
            return self.proc.pid

    @property
    def returncode(self):
        if self.proc:
            return self.proc.returncode


class SshClient(RunProcess):
    def __init__(self, cmd, server, username=None, identity_file=None, timeout=None,
                 name=None, encoding='utf-8',
                 output_file=None, as_root=False, nice=None, verbose=True, password=None):
        self.remote_cmd_str, cmd_args = shell_command_line(cmd, as_root=as_root, nice=nice,
                                                           make_sendable=False, is_local=False)
        if password is not None:
            ssh_cmd_args = ['sshpass', '-p', str(password), *SSH_CMD_ARGS]
        else:
            ssh_cmd_args = SSH_CMD_ARGS.copy()

        if identity_file:
            ssh_cmd_args += ['-i', str(identity_file)]

        if timeout:
            ssh_cmd_args += ["-o", f"ConnectTimeout={timeout}"]

        if username is None:
            self.server_address = server
        else:
            self.server_address = f"{username}@{server}"

        ssh_cmd_args.append(self.server_address)
        ssh_cmd_args.append(self.remote_cmd_str)

        RunProcess.__init__(self, ssh_cmd_args, encoding, output_file, name=name, verbose=verbose)

    def log_start(self):
        self.logger.debug("Running '%s' on '%s'", self.remote_cmd_str, self.server_address)

    def log_end(self):
        self.logger.debug("Remote SSH ended")


class RsyncClient(RunProcess):
    def __init__(self, source, destination, server, username=None,
                 parameters=None, exclude=None, include=None, delete=False,
                 ssh_identity_file=None, ssh_timeout=None,
                 name=None, encoding='utf-8', verbose=True, password=None):
        self.source = source
        self.destination = destination

        ssh_cmd_args = SSH_CMD_ARGS.copy()
        if ssh_identity_file:
            ssh_cmd_args += ['-i', str(ssh_identity_file)]
        if ssh_timeout:
            ssh_cmd_args += ["-o", f"ConnectTimeout={ssh_timeout}"]

        if username is None:
            server_address = server
        else:
            server_address = f"{username}@{server}"

        self.destination_address = f"{server_address}:{destination}"

        if parameters is None:
            parameters = []
        if delete:
            parameters.append('--delete')
        if include is not None:
            parameters.extend(itertools.chain.from_iterable(('--include', e) for e in include))
        if exclude is not None:
            parameters.extend(itertools.chain.from_iterable(('--exclude', e) for e in exclude))

        rsync_cmd_args = ['rsync', '-e', ' '.join(ssh_cmd_args), '-azW', *parameters, source, self.destination_address]
        if password is not None:
            rsync_cmd_args = ['sshpass', '-p', str(password), *rsync_cmd_args]

        RunProcess.__init__(self, rsync_cmd_args, encoding, name=name, verbose=verbose)

    def log_start(self):
        self.logger.debug("Rsync '%s' to '%s'", self.source, self.destination_address)

    def log_end(self):
        self.logger.debug("Rsync ended")

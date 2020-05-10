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
import socket
import getpass
import itertools
import subprocess
from os.path import expanduser

from configparser import ConfigParser, ExtendedInterpolation

import cloudexp

NAME = "elasticbench"
CONFIG_FILE = os.path.abspath(os.path.expanduser(f"~/.{NAME}"))
DEFAULT_WORKSPACE = os.path.abspath(os.path.expanduser("~/workspace"))
DEFAULT_DATA_PATH = DEFAULT_WORKSPACE

DEFAULT_CONFIG = """
# ElasticBenchmarks: data paths of the experiments framework.

[main]
# The default data path may be used as a parent for other data paths.
# It is only used by the other paths.
default_data_path = %(default_data_path)s
default_workspace = %(default_workspace)s

# The name of the experiments
name = %(name)s

# The directory in which we store our experiments' results.
output_path = ${default_data_path}/${name}

# The directory to clone app repositories
applications_bin = ${default_workspace}/${name}-apps

monitor_server_address = localhost:5050
""" % dict(name=NAME, default_data_path=DEFAULT_DATA_PATH, default_workspace=DEFAULT_WORKSPACE)


def default_config():
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read_string(DEFAULT_CONFIG)
    return config


def init_config_file():
    config = default_config()
    with open(CONFIG_FILE, 'w') as f:
        config.write(f)


def read_config_param(key):
    if os.path.isfile(CONFIG_FILE):
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read(CONFIG_FILE)
    else:
        config = default_config()

    return config.get('main', key)


def user_home():
    if 'SUDO_USER' in os.environ:
        return f"/home/{os.environ['SUDO_USER']}"
    else:
        return expanduser("~")


def username():
    return getpass.getuser()


def revision_info():
    return dict(
        id=subprocess.check_output(['hg', 'id', '--id']).strip(),
        branch=subprocess.check_output(['hg', 'id', '--branch']).strip(),
        tags=subprocess.check_output(['hg', 'id', '--tags']).strip()
    )


def hostname():
    return socket.gethostname()


def get_conf_path():
    module_path = os.path.dirname(__file__)
    repo_path = os.path.dirname(module_path)
    return os.path.join(repo_path, 'conf')


def get_conf_file(file_name):
    conf_path = get_conf_path()
    return os.path.join(conf_path, file_name)


def get_guest_repository_path(file_path):
    return os.path.join('~', cloudexp.get_repository_relative_path(file_path))


def read_output_path():
    return expanduser(read_config_param("output_path"))


def read_applications_bin():
    return expanduser(read_config_param("applications_bin"))


def output_path(*subpath):
    return os.path.join(read_output_path(), *subpath)


def relative_output_path(*subpath):
    return os.path.join(*subpath)


def get_output_path_and_relative(*subpath):
    rel_path = relative_output_path(*subpath)
    return rel_path, output_path(*subpath)


def get_application_path(*subpath):
    return os.path.join(read_applications_bin(), *subpath)


def monitor_server_address():
    return read_config_param('monitor_server_address')


def linkify_to_monitor(*subpath):
    monitor_address = monitor_server_address()
    rel_path = os.path.join(*subpath)
    from IPython.core.display import HTML
    return HTML(f'<a href="{monitor_address}#{rel_path}">{rel_path}</a>')



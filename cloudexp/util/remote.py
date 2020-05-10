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
import sys
import pickle
from typing import Callable
from subprocess import Popen


def remote_process_function_call(process_object: Popen, remote_function: Callable, *args, **kwargs):
    data = [remote_function, args, kwargs]
    pickle.dump(data, process_object.stdin)


def remote_function_handler():
    remote_function, args, kwargs = pickle.load(sys.stdin.buffer)
    remote_function(*args, **kwargs)


if __name__ == '__main__':
    remote_function_handler()

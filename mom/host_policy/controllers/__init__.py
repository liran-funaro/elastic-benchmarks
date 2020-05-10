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
import pkgutil
import importlib
import warnings


CONTROLLERS = set([modname for _, modname, _ in pkgutil.iter_modules(sys.modules[__name__].__path__)])


def get_controller(controller_name):
    if not isinstance(controller_name, str):
        raise TypeError(f"Controller name must be a string. Not {controller_name}. "
                        f"Choose one of the followings: {CONTROLLERS}.")

    if controller_name not in CONTROLLERS:
        raise ValueError(f"No such controller: {controller_name}. Choose one of the followings: {CONTROLLERS}.")

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=ImportWarning)
        controller_module = importlib.import_module(f'.{controller_name}', __name__)

    return getattr(controller_module, controller_name)

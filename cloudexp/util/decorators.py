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
import pprint


def dict_represented(cls):
    """
    A class that implements the method @property as_dict(self) can use this decorator
    to automatically implement 2 methods:
      1. __repr__(self): will return the class representation as string based
              on the result from as_dict().
      2. write_info(self, out_dir): writes the result of as_dict() in an info
              file located in the out_dir path.
    """
    cls.__INFO_DEFAULT_FILE_NAME__ = "info"

    def dict_repr(self_obj):
        return pprint.pformat(self_obj.as_dict())

    def write_info(self_obj, out_dir):
        with open(os.path.join(out_dir, self_obj.__INFO_DEFAULT_FILE_NAME__), "w") as f:
            f.write(pprint.pformat(self_obj))

    cls.__repr__ = dict_repr
    cls.write_info = write_info

    return cls


def synchronized(func):
    """ This decorator will make a function to synchronized with a global class lock """
    def synced_func(self, *args, **kws):
        with self._lock:
            return func(self, *args, **kws)

    synced_func.__name__ = func.__name__

    return synced_func


class Singleton(type):
    """
    A singleton class as suggested here:
    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    Usage:
    =====================================================

    In Python2:
    -----------
    class MyClass(BaseClass):
        __metaclass__ = Singleton

    In Python3:
    -----------
    class MyClass(BaseClass, metaclass=Singleton):
        pass
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class FunctionRepresentation(object):
    """ Allow to add string representation to a function (i.e. tostring) """

    def __init__(self, func, representation):
        self.func = func
        self.representation = representation

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return "'%s'" % self.representation


def function_repr(representation):
    def wrapper(func):
        return FunctionRepresentation(func, representation)
    return wrapper

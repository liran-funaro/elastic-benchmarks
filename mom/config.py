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
import copy
import sys
import pkgutil
import importlib
import warnings
from typing import Union, Iterable

from mom.logged_object import LoggedObject


DictConfigType = Union['DictConfig', dict, None]

DEFAULT_VALUE = object()


class DictConfig:
    def __init__(self, *priority_init_param: DictConfigType):
        self.__data__ = {}
        for p in priority_init_param:
            if isinstance(p, dict):
                self.read_dict(p)
            elif isinstance(p, DictConfig):
                self.read_dict(p.__data__)
            elif p is None:
                continue
            else:
                raise ValueError(f"Initialization parameters must be a dict or DictConfig. Got {p}.")

    def get_dict(self):
        return self.__data__

    def read_dict(self, e=None, **f):
        update_dict = dict(e, **f)
        for section, parameter in update_dict.items():
            if type(section) not in (tuple, list, str):
                raise ValueError(f"Section must be a tuple, list or string. Got {section}.")
            if type(section) in (tuple, list):
                section, key = section
            elif not isinstance(parameter, dict):
                section, key = 'main', section
            else:
                key = None
            sec = self.add_section(section)

            if key is None:
                for key, value in parameter.items():
                    sec[key] = copy.copy(value)
            else:
                sec[key] = copy.copy(parameter)

    def has_section(self, section: str):
        return section in self.__data__

    def has_key(self, section: str, key: str):
        return key in self.__data__.get(section, {})

    def add_section(self, section: str):
        assert isinstance(section, str)
        return self.__data__.setdefault(section, {})

    def set(self, section: str, key: str, value):
        assert isinstance(section, str)
        assert isinstance(key, str)
        self.add_section(section)[key] = value

    def get(self, section: str, key: str, default_value=DEFAULT_VALUE):
        assert isinstance(section, str)
        assert isinstance(key, str)
        sec = self.__data__.get(section, DEFAULT_VALUE)
        if sec is DEFAULT_VALUE and default_value is DEFAULT_VALUE:
            raise KeyError(f"No section '{section}'.")
        elif sec is DEFAULT_VALUE:
            return default_value

        val = sec.get(key, default_value)
        if val is DEFAULT_VALUE:
            raise KeyError(f"No key '{key}' in section '{section}'.")
        return val

    def __getitem__(self, item: tuple):
        assert isinstance(item, tuple), 'Item must be a tuple: (section, key)'
        return self.get(item[0], item[1])


class ClassImporter(LoggedObject):
    def __init__(self, module):
        self.module_name = module.__name__
        LoggedObject.__init__(self, self.module_name)
        self._class_key = None

    def import_sub_module(self, sub_module_name: str):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=ImportWarning)
            return importlib.import_module(f'.{sub_module_name}', self.module_name)

    @property
    def class_key(self):
        if self._class_key is not None:
            return self._class_key

        module_path = sys.modules[self.module_name].__path__
        all_modules_names = set([modname for _, modname, _ in pkgutil.iter_modules(module_path)])
        all_modules_names.add('')
        all_modules = []
        for m_name in all_modules_names:
            try:
                all_modules.append(self.import_sub_module(m_name))
            except Exception as e:
                self.logger.exception("Failed to import submodule '%s': %s", m_name, e)

        self._class_key = {}
        for m in all_modules:
            candidates = {name: cls for name, cls in m.__dict__.items() if isinstance(cls, type)}
            for name, cls in candidates.items():
                if cls.__module__.startswith(self.module_name):
                    self._class_key[name] = cls
        return self._class_key

    def get_class(self, class_name: str):
        if isinstance(class_name, type):
            return class_name
        if not isinstance(class_name, str):
            raise TypeError(f"Class must be a type or a string. Not {class_name}. "
                            f"Choose one of the followings: {tuple(self.class_key)}.")

        ret_class = self.class_key.get(class_name.strip(), None)
        if ret_class is None:
            raise ValueError(f"No such class: {ret_class}. Choose one of the followings: {tuple(self.class_key)}.")
        return ret_class

    def get_all_classes(self):
        return self.class_key.values()

    def iter_list_of_classes(self, class_list: Iterable):
        for c in class_list:
            try:
                yield self.get_class(c)
            except Exception as e:
                self.logger.exception("Failed to find class '%s': %s", c, e)

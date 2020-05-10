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
import re
from ast import literal_eval


################################################################################################################
# Parse string parameters
################################################################################################################

def parameter_str(param, default=None):
    try:
        return str(param)
    except (TypeError, ValueError):
        return default


def parameter_int(param, default=None):
    try:
        return int(param)
    except (TypeError, ValueError):
        return default


def parameter_float(param, default=None):
    try:
        return float(param)
    except (TypeError, ValueError):
        return default


def parameter_int_or_float(param, default=None):
    fail = object()
    ret = parameter_int(param, fail)
    if ret is not fail:
        return ret
    return parameter_float(param, default)


def parameter_int_or_float_or_str(param, default=None):
    fail = object()
    ret = parameter_int(param, fail)
    if ret is not fail:
        return ret
    ret = parameter_float(param, fail)
    if ret is not fail:
        return ret
    return parameter_str(param, default)


def parameter_bool(param, default=None):
    if isinstance(param, bool):
        return param
    elif isinstance(param, str):
        try:
            return literal_eval(param)
        except (TypeError, ValueError, SyntaxError):
            return default
    else:
        return default


def parameter_dict(param, default=None):
    if isinstance(param, dict):
        return param
    elif isinstance(param, str):
        try:
            return literal_eval(param)
        except (TypeError, ValueError, SyntaxError):
            return default
    else:
        return default


################################################################################################################
# Parse using regular expression
################################################################################################################

def parse_type(pattern, src: str, type_func=parameter_str, default=None):
    """
    Parse a body of text according to the provided regular expression and return
    the first match as the type using the type_func.
    """
    m = re.search(pattern, src, re.M | re.I)
    if not m:
        return default
    return type_func(m.group(1), default)


def parse_string(pattern, src: str, default=None):
    return parse_type(pattern, src, parameter_str, default)


def parse_int(pattern, src: str, default=None):
    return parse_type(pattern, src, parameter_int, default)


def parse_float(pattern, src: str, default=None):
    return parse_type(pattern, src, parameter_float, default)


def parse_int_or_float(pattern, src: str, default=None):
    return parse_type(pattern, src, parameter_int_or_float, default)


def parse_type_list(pattern, src: str, type_func=parameter_str, default=None):
    match_iter = re.finditer(pattern, src, re.M | re.I)
    if not match_iter:
        return default
    return [type_func(m.group(1), default) for m in match_iter]


def parse_int_list(pattern, src: str, default=None):
    return parse_type_list(pattern, src, parameter_int, default)


def parse_float_list(pattern, src: str, default=None):
    return parse_type_list(pattern, src, parameter_float, default)


def parse_int_or_float_list(pattern, src: str, default=None):
    return parse_type_list(pattern, src, parameter_int_or_float, default)

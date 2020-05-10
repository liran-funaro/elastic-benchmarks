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
import math
from typing import Union

TIME_ANNOTATIONS = {
    'long': {
        's': ' seconds',
        'm': ' minutes',
        'h': ' hours',
        'd': ' days',
    },
    'short': {
        's': 's',
        'm': 'm',
        'h': 'h',
        'd': 'd',
    }
}

# Time units
SECONDS = 1
MINUTES = 60
HOURS = MINUTES * 60
DAYS = HOURS * 24

UNITS_TRANSLATE = {
    **dict.fromkeys((SECONDS, 's', 'sec', 'second', 'seconds'), SECONDS),
    **dict.fromkeys((MINUTES, 'm', 'min', 'minute', 'minutes'), MINUTES),
    **dict.fromkeys((HOURS, 'h', 'hr', 'hour', 'hours'), HOURS),
    **dict.fromkeys((DAYS, 'd', 'dy', 'day', 'days'), DAYS),
}

Unit = Union[str, int, None]
Time = Union[int, float]


def get_unit_factor(unit: Unit = None, default_time_unit: Unit = SECONDS) -> int:
    if unit is None:
        return default_time_unit
    unit_factor = UNITS_TRANSLATE.get(unit, None)
    if unit_factor is None:
        raise ValueError(f"Unit must be one of the following: {tuple(UNITS_TRANSLATE.keys())}. "
                         f"Got {unit}.")
    return unit_factor


def convert_to_seconds(time: Time, unit: Unit = None, default_time_unit: Unit = SECONDS) -> Time:
    unit_factor = get_unit_factor(unit, default_time_unit)
    return time * unit_factor


def time_delta(input_time_delta, units='seconds', time_annotations='long', show_seconds=False):
    time_seconds = convert_to_seconds(input_time_delta, units)
    annot = TIME_ANNOTATIONS[time_annotations]

    sign = ''
    if time_seconds < -1.5e-2:
        time_seconds = -time_seconds
        sign = '-'

    if time_seconds < 1e-3:
        return '0'
    elif time_seconds < 60:
        time_seconds = round(time_seconds, 2)
        return f"{sign}{time_seconds:.4g}{annot['s']}"
    elif time_seconds < (60 * 60):
        sub_minute, minutes = math.modf(time_seconds / 60)
        seconds = sub_minute * 60
        if seconds >= 1:
            return f"{sign}{minutes:.0f}:{seconds:02.0f}{annot['m']}"
        else:
            return f"{sign}{minutes:.0f}{annot['m']}"
    elif time_seconds < (60 * 60 * 24):
        sub_hour, hours = math.modf(time_seconds / (60 * 60))
        sub_minute, minutes = math.modf(sub_hour * 60)
        seconds = sub_minute * 60

        if show_seconds and seconds > 1:
            return f"{sign}{hours:.0f}:{minutes:02.0f}:{seconds:02.0f}{annot['h']}"
        elif minutes >= 1:
            return f"{sign}{hours:.0f}:{minutes:02.0f}{annot['h']}"
        else:
            return f"{sign}{hours:.0f}{annot['h']}"
    else:
        hours = time_seconds / (60 * 60)
        days = int(hours / 24)
        hours = hours - (24 * days)
        sub_hour, hours = math.modf(hours)
        sub_minute, minutes = math.modf(sub_hour * 60)
        seconds = sub_minute * 60
        if show_seconds and seconds > 1:
            return f"{sign}{days}{annot['d']}, {hours:.0f}:{minutes:02.0f}:{seconds:02.0f}{annot['h']}"
        if hours >= 1 and minutes >= 1:
            return f"{sign}{days}{annot['d']}, {hours:.0f}:{minutes:02.0f}{annot['h']}"
        elif hours >= 1:
            return f"{sign}{days}{annot['d']}, {hours:.0f}{annot['h']}"
        elif minutes >= 1:
            return f"{sign}{days}{annot['d']}, {minutes:02.0f}{annot['m']}"
        else:
            return f"{sign}{days}{annot['d']}"


def throughput(input_throughput, items='items', units='seconds'):
    throughput_seconds = convert_to_seconds(input_throughput, units)

    if throughput_seconds > 1:
        return f"{throughput_seconds:.2f} {items}/second"
    elif throughput_seconds * 60 > 1:
        return f"{throughput_seconds * 60:.2f} {items}/minute"
    elif throughput_seconds * 60 * 60 > 1:
        return f"{throughput_seconds * 60 * 60:.2f} {items}/hour"
    else:
        return f"{throughput_seconds * 24 * 60 * 60:.2f} {items}/day"

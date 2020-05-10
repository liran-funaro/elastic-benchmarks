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
import bisect
from typing import Union, Hashable, List, Tuple, Dict, Optional, Callable
from cloudexp.util.timeformat import time_delta, get_unit_factor, convert_to_seconds, \
    SECONDS, MINUTES, HOURS, DAYS, Unit, Time

Value = Hashable
Number = Union[int, float]

REPRESENTATIONS = {
    'long': '%(full_repr)s',
    'short': '%(duration)s: %(min_value)s to %(max_value)s',
    'static': '%(duration)s: %(min_value)s',
}


class FunctionOfTime:
    """ An functor for time to value function """
    def __init__(self, value_duration_tuples=(),
                 default_time_unit: Unit = None, format_value: str = "{}",
                 default_representation=None, output_with_interval_time=False):
        self.default_time_unit = SECONDS
        self.default_time_unit = self.get_unit_factor(default_time_unit)

        self.format_value = format_value

        self.default_representation = REPRESENTATIONS['long']
        self.set_default_representation(default_representation)

        self.output_with_interval_time = output_with_interval_time

        self.times: List[Time] = []
        self.values: List[Tuple[Value, Time]] = []

        self.next_time = 0

        for arg in value_duration_tuples:
            self.add_value(*arg)

    def get_unit_factor(self, unit: Unit = None) -> int:
        return get_unit_factor(unit, self.default_time_unit)

    def convert_to_seconds(self, time: Time, unit: Unit = None) -> Time:
        return convert_to_seconds(time, unit, self.default_time_unit)

    def get_representation_template(self, representation: Optional[str] = None) -> str:
        if representation is None:
            return self.default_representation
        if not isinstance(representation, str):
            raise ValueError(f"Representation must be a template string.Got {representation}.")
        return REPRESENTATIONS.get(representation, representation)

    def set_default_representation(self, default_representation=None):
        self.default_representation = self.get_representation_template(default_representation)

    def add_value(self, value: Value, duration: Time, unit: Unit = None) -> None:
        assert duration > 0, f'In tuple: ({value, duration, unit}), duration is less than zero.'
        duration_in_seconds = self.convert_to_seconds(duration, unit)

        self.times.append(self.next_time)
        self.values.append((value, duration_in_seconds))
        self.next_time += duration_in_seconds

    def get_min_value(self) -> Value:
        return min(self.values)[0]

    def get_max_value(self) -> Value:
        return max(self.values)[0]

    def get_index_for_time(self, time: Time, unit: Unit = None) -> int:
        time_in_seconds = self.convert_to_seconds(time, unit)
        i = bisect.bisect_right(self.times, time_in_seconds) - 1
        if i > 0:
            return i
        else:
            return 0

    def get(self, time: Time, unit: Unit = None) -> Tuple[Value, Time]:
        i = self.get_index_for_time(time, unit)
        return self.values[i]

    def get_value(self, time: Time, unit: Unit = None) -> Value:
        value, duration = self.get(time, unit)
        return value

    def get_duration(self, time: Time, unit: Unit = None) -> Time:
        value, duration = self.get(time, unit)
        return duration

    def get_max_time(self, unit: Unit = None) -> Time:
        unit_factor = self.get_unit_factor(unit)
        if unit_factor == 1:
            return self.next_time
        else:
            return self.next_time / unit_factor

    def __call__(self, time: Time, unit: Unit = None) -> Union[Value, Tuple[Value, Time]]:
        value, duration = self.get(time, unit)
        if self.output_with_interval_time:
            return value, duration
        else:
            return value

    def get_full_repr(self) -> str:
        return ", ".join([f"{time_delta(t, time_annotations='short')}->{self.format_value.format(v[0])}" for t, v in
                          zip(self.times, self.values)])

    def get_max_time_repr(self) -> str:
        return time_delta(self.get_max_time(SECONDS), time_annotations='short')

    def get_repr_dict(self) -> Dict[str, str]:
        return dict(
            full_repr=self.get_full_repr(),
            duration=self.get_max_time_repr(),
            max_value=self.format_value.format(self.get_max_value()),
            min_value=self.format_value.format(self.get_min_value()),
        )

    def get_representation(self, representation: Optional[str] = None) -> str:
        repr_template = self.get_representation_template(representation)
        repr_dict = self.get_repr_dict()
        return repr_template % repr_dict

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.get_representation()})'

    def plot(self, ax=None):
        x = [*self.times, self.times[-1] + self.values[-1][1]]
        y = [*(v for v, d in self.values), self.values[-1][0]]
        from matplotlib.pylab import plt
        if ax is None:
            ax = plt.gca()
        ax.step(x, y, where='post')
        ax.set_xlabel('Time (seconds)')
        y_ticks = list(map(int, plt.yticks()[0]))
        ax.set_yticks(y_ticks, list(map(self.format_value.format, y_ticks)))

    ####################################################################################################################
    # Manipulation
    ####################################################################################################################

    def apply(self, func: Callable, inplace=False):
        if inplace:
            applied_self = self
        else:
            applied_self = copy.deepcopy(self)
        applied_self.values = [(func(value), duration) for value, duration in self.values]
        return applied_self

    def concat(self, another: 'FunctionOfTime', inplace=False):
        if inplace:
            applied_self = self
        else:
            applied_self = copy.deepcopy(self)

        applied_self.values.extend(another.values)
        applied_self.times.extend([applied_self.next_time + t for t in another.times])
        applied_self.next_time += another.next_time
        return applied_self

    def astype(self, type_func: Union[type, Callable], inplace=False):
        return self.apply(type_func, inplace=inplace)

    def clip(self, min_value=None, max_value=None, inplace=False):
        return self.apply(lambda x: max(min_value, min(max_value, x)), inplace=inplace)

    def add(self, number: Number, inplace=False):
        return self.apply(lambda x: x + number, inplace=inplace)

    def __add__(self, number: Number):
        return self.add(number)

    def __iadd__(self, number: Number):
        return self.add(number, inplace=True)

    def __radd__(self, number: Number):
        return self.add(number)

    def subtract(self, number: Number, inplace=False):
        return self.apply(lambda x: x - number, inplace=inplace)

    def __sub__(self, number: Number):
        return self.subtract(number)

    def __isub__(self, number: Number):
        return self.subtract(number, inplace=True)

    def __rsub__(self, number: Number):
        return self.apply(lambda x: number - x)

    def multiply(self, number: Number, inplace=False):
        return self.apply(lambda x: x * number, inplace=inplace)

    def __mul__(self, number: Number):
        return self.multiply(number)

    def __imul__(self, number: Number):
        return self.multiply(number, inplace=True)

    def __rmul__(self, number: Number):
        return self.multiply(number)

    def divide(self, number: Number, inplace=False):
        return self.apply(lambda x: x / number, inplace=inplace)

    def __truediv__(self, number: Number):
        return self.divide(number)

    def __itruediv__(self, number: Number):
        return self.divide(number, inplace=True)

    def __rtruediv__(self, number: Number):
        return self.apply(lambda x: number / x)

    def floordiv(self, number: Number, inplace=False):
        return self.apply(lambda x: x // number, inplace=inplace)

    def __floordiv__(self, number: Number):
        return self.floordiv(number)

    def __ifloordiv__(self, number: Number):
        return self.floordiv(number, inplace=True)

    def __rfloordiv__(self, number: Number):
        return self.apply(lambda x: number // x)


def static_function(alloc_arg=(), default_time_unit: Unit = None,
                    format_value="{}", default_representation='static', output_with_interval_time=False):
    ret = FunctionOfTime((), default_time_unit, format_value, default_representation,
                         output_with_interval_time)
    ret.add_value(*alloc_arg)
    return ret

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
import math
import numpy as np

from cloudexp.util.function_of_time import FunctionOfTime, static_function


def load_function(*args, format_value='load:{:,}', **kwargs):
    return FunctionOfTime(*args, format_value=format_value, **kwargs)


def constant_load(val, duration=(1, 'm'), format_value='load:{:,}', **kwargs):
    return static_function((val, *duration), format_value=format_value, **kwargs)


################################################################################################################
# Older load function definition
# @author: eyal
################################################################################################################


class LoadFunction(object):
    def __call__(self, t):
        raise NotImplementedError

    @property
    def info(self):
        raise NotImplementedError

    def __repr__(self):
        return repr(self.info)


class LoadTrace(LoadFunction):
    def __init__(self, time, vals):
        self.t = np.array(time)
        self.vals = np.array(vals)

    def __call__(self, t):
        i = np.argmin(np.abs(self.t - t))
        #        i = min(np.argmax(self.t >= t) + 1, len(self.vals) - 1)
        return self.vals[i]
        # return np.interp(t, self.t, self.vals, left = self.vals[0], right = self.vals[-1])

    def __repr__(self):
        return "Loads.LoadTrace(time=%s,vals=%s)" % (str(list(self.t)), str(list(self.vals)))

    @property
    def info(self):
        return {"times": self.t, "vals": self.vals}


class LoadSine(LoadFunction):
    def __init__(self, a, t, p, offset):
        self.a = float(a)
        self.T = float(t)
        self.p = float(p)
        self.offset = float(offset)

    def __call__(self, t):
        return max(0., self.offset + self.a * math.sin(math.pi * 2 / self.T * (float(t) - self.p)))

    @property
    def info(self):
        return {"type": self.__class__.__name__,
                "a": self.a, "T": self.T, "p": self.p, "offset": self.offset}


class LoadBinary(LoadFunction):
    """
    v1 - first value
    v2 - second value
    T - half period time
    T0 - phase (starting position)
    """

    def __init__(self, v1, v2, t, t0):
        self.v1 = int(v1)
        self.v2 = int(v2)
        self.T = int(t)
        self.T0 = int(t0)

    def __call__(self, t):
        return self.v1 if ((int(t) - self.T0) / self.T) % 2 == 0 else self.v2

    @property
    def info(self):
        return {"type": self.__class__.__name__,
                "v1": self.v1, "v2": self.v2, "T": self.T, "T0": self.T0}


def gen_random_sine(n, _t_avg=300, amp_std=1, amp_avg=3.5, load_avg=8):
    t_std = 300 / 10

    amplitude = amp_avg + amp_std * np.random.randn(n)
    t = 300 + t_std * np.random.randn(n)
    phase = np.random.uniform(0, 300, size=n)

    funcs = []
    for i in range(n):
        funcs.append(LoadSine(amplitude[i], t[i], phase[i], load_avg))

    return funcs


def gen_random_sine_const_sum(n, load_avg, amp_avg, amp_std, t_avg):
    t_std = t_avg / 10
    phase_std = t_std
    m = n / 2
    aas = amp_avg + amp_std * np.random.randn(m)
    ts = t_avg + t_std * np.random.randn(m)
    phase = phase_std * np.random.randn(m)
    funcs = []
    for i in range(m):
        funcs.append(LoadSine(aas[i], ts[i], phase[i], load_avg))
        funcs.append(LoadSine(aas[i], ts[i], phase[i] + 0.5 * ts[i], load_avg))

    if n % 2 != 0:  # add odd function
        funcs.append(constant_load(load_avg))

    return funcs


def gen_random_binary_const_sum(n, load_avg, amp_avg, amp_std, t_avg):
    t_std = t_avg / 10
    m = n / 2
    aas = amp_avg + amp_std * np.random.randn(m)
    ts = t_avg + t_std * np.random.randn(m)
    phases = t_std * np.random.randn(m)
    funcs = []
    for i in range(m):
        funcs.append(LoadBinary(load_avg + aas[i], load_avg - aas[i], ts[i], phases[i]))
        funcs.append(LoadBinary(load_avg - aas[i], load_avg + aas[i], ts[i], phases[i]))

    if n % 2 != 0:  # add odd function
        funcs.append(constant_load(load_avg))

    return funcs


def gen_const_const_sum(n, load_avg, _amp_avg, amp_std, _t_avg):
    m = n / 2
    aas = amp_std * np.random.randn(m)
    funcs = []
    for i in range(m):
        funcs.append(constant_load(load_avg + aas[i]))
        funcs.append(constant_load(load_avg - aas[i]))

    if n % 2 != 0:  # add odd function
        funcs.append(constant_load(load_avg))

    return funcs


def gen_const_different(n, load_avg, amp_avg, _amp_std, _t_avg):
    m = n / 2
    aas = [(x + 1) * amp_avg / m for x in range(m)]
    funcs = []
    for i in range(m):
        funcs.append(constant_load(load_avg + aas[i]))
        funcs.append(constant_load(load_avg - aas[i]))

    if n % 2 != 0:  # add odd function
        funcs.append(constant_load(load_avg))

    return funcs


def gen_binary_const_sum(n, load_avg, amp_avg, _amp_std, t_avg):
    t_std = t_avg / 10
    funcs = []
    m = n / 2
    if m > 0:
        _d = float(amp_avg) / m
        aas = [int(x) for x in np.linspace(1, amp_avg, m)]
        ts = t_avg + t_std * np.random.randn(m)
        phases = t_std * np.random.randn(m)
        for i in range(m):
            funcs.append(LoadBinary(load_avg + aas[i], load_avg - aas[i],
                                    ts[i], phases[i]))
            funcs.append(LoadBinary(load_avg - aas[i], load_avg + aas[i],
                                    ts[i], phases[i]))

    if n % 2 != 0:  # add odd function
        funcs.append(constant_load(load_avg))

    return funcs


def gen_binary_low_low_mem_const_sum(n, load_avg, amp_avg, _amp_std, t_avg):
    t_std = t_avg / 10
    funcs = []
    m = n / 4
    if m > 0:
        aas = [int(x) for x in np.linspace(1, amp_avg, m)]
        ts = t_avg + t_std * np.random.randn(m)
        phases = t_std * np.random.randn(m)
        for i in range(m):
            funcs.append(LoadBinary(load_avg + aas[i], 1, ts[i], phases[i]))
            funcs.append(LoadBinary(1, load_avg + aas[i], ts[i], phases[i]))

            funcs.append(LoadBinary(load_avg - aas[i], 1, ts[i], phases[i]))
            funcs.append(LoadBinary(1, load_avg - aas[i], ts[i], phases[i]))

    if n % 2 != 0:  # add odd function
        funcs.append(constant_load(load_avg))

    return funcs


loadupto7 = {"load_avg": 4, "amp_avg": 3, "amp_std": 0}
loadupto10 = {"load_avg": 6, "amp_avg": 4, "amp_std": 0}
loadupto11 = {"load_avg": 6, "amp_avg": 5, "amp_std": 0}
loadupto20 = {"load_avg": 11, "amp_avg": 9, "amp_std": 0}
loadupto30 = {"load_avg": 15, "amp_avg": 8, "amp_std": 0}
loadupto40 = {"load_avg": 20, "amp_avg": 9, "amp_std": 0}
loadupto60 = {"load_avg": 33, "amp_avg": 20, "amp_std": 2}
loadupto80 = {"load_avg": 40, "amp_avg": 24, "amp_std": 0}
loadupto150 = {"load_avg": 75, "amp_avg": 42, "amp_std": 0}
loadupto200 = {"load_avg": 100, "amp_avg": 69, "amp_std": 0}

# loadupto15 = {"load_avg": 8, "amp_avg": 3.5, "amp_std": 1}
# loadupto20 = {"load_avg": 11, "amp_avg": 4.5, "amp_std": 1.5}
# loadupto30 = {"load_avg": 15, "amp_avg": 8, "amp_std": 2}
# loadupto40 = {"load_avg": 20, "amp_avg": 9, "amp_std": 2}
# loadupto60 = {"load_avg": 32, "amp_avg": 17, "amp_std": 4}
# loadupto80 = {"load_avg": 40, "amp_avg": 24, "amp_std": 5}
# loadupto150 = {"load_avg": 75, "amp_avg": 42, "amp_std": 10}
# loadupto200 = {"load_avg": 100, "amp_avg": 69, "amp_std": 10}


def test():
    import pylab as pl

    n = 1
    #    funcs = gen_const_different(n, T_avg = 200, **loadupto11)
    #    funcs = gen_binary_const_sum(n, 30, 20, 0, T_avg = 200)
    funcs = [LoadBinary(v1=1, v2=0, t=10, t0=3)]
    t = np.array(list(range(15)))
    tot = np.zeros_like(t)
    for i in range(n):
        y = np.array(list(map(funcs[i], t)))
        tot += y
        pl.plot(t, y, "x")

    #    pl.plot(t, tot, linewidth = 3, color = "k")

    pl.show()


if __name__ == "__main__":
    test()

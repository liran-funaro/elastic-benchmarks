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
from typing import Union


Memory = Union[int, float]

InvalidMemory = InvMem = float('nan')
UnlimitedMemory = UnMem = float('inf')


def is_valid_mem(mem: Memory):
    """ Verify an object is a valid memory """
    return type(mem) in (int, float) and not math.isinf(mem) and not math.isnan(mem)


def is_memory_close(mem1: Memory, mem2: Memory, eps: Memory = 1):
    """ Calculate if two memory allocations are similar up to epsilon (eps) """
    return is_valid_mem(mem1) and is_valid_mem(mem2) and (mem1 + eps > mem2) and (mem2 + eps > mem1)

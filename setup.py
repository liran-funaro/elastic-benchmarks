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
import os
from setuptools import setup

all_scripts = ['ssh-vm.sh', 'query-python.sh', 'query-cpu-affinity.sh', 'htop-qemu.sh']

setup(
    name='elasticbench',
    version="0.1.0",
    packages=['mom', 'cloudexp', 'elasticbench'],
    description='Elastic Benchmarking using MOM.',
    author="Liran Funaro",
    author_email="liran.funaro+elasticbench@gmail.com",
    long_description=open('README.md').read(),
    url="https://github.com/liran-funaro/elastic-benchmarks",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=['numpy', 'msgpack', 'msgpack-numpy', 'matplotlib', 'scipy', 'pandas', 'bokeh', 'seaborn',
                      'tables', 'psutil', 'flask', 'nesteddict'],
    dependency_links=['https://github.com/liran-funaro/nesteddict'],
    scripts=[os.path.join('scripts', s) for s in all_scripts],
)

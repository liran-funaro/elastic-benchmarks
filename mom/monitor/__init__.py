# Memory Overcommitment Manager
# Copyright (C) 2010 Adam Litke, IBM Corporation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
import time
import threading
from collections import deque

from mom.util.terminable import Terminable

from mom.config import DictConfig, ClassImporter
from mom.monitor import collectors
from mom.logged_object import LoggedObject
from mom.util.data_logger import DataLogger
from mom.util.dict_itertools import dict_recursive_update


class Monitor(Terminable, LoggedObject):
    """
    The Monitor class represents an entity, about which, data is collected and
    reported.  Each monitor has a dictionary of properties which are relatively
    static such as a name or ID.  Additionally, statistics are collected over
    time and queued so averages and trends can be analyzed.
    """
    def __init__(self, config: DictConfig, monitor_name=None, monitor_source=None,
                 shared_terminable: Terminable = None):
        Terminable.__init__(self, shared_terminable=shared_terminable)
        LoggedObject.__init__(self, monitor_name)
        # Guard the data with a semaphore to ensure consistency.
        self.data_lock = threading.Lock()
        self.statistics = deque()
        self.variables = {}
        self.monitor_name = monitor_name
        self.monitor_source = monitor_source
        self.data_logger = DataLogger('monitor', monitor_source)
        self.ready = threading.Event()
        self.config = config
        self.properties = self._properties()
        self.collectors = self._generate_collectors()

        self.hist_len = self.config.get('monitor', 'sample-history-length')

    def _properties(self):
        """ Can be overridden """
        return {"config": self.config, "name": self.monitor_name, "source": self.monitor_source}

    def _collectors_list(self, config):
        """ Can be overridden """
        return config.get('monitor', 'collectors')

    def _generate_collectors(self):
        collector_list = self._collectors_list(self.config)
        ret = []
        for collector_class in ClassImporter(collectors).iter_list_of_classes(collector_list):
            try:
                ret.append(collector_class(self.properties))
            except Exception as e:
                self.logger.exception("Failed to initiate collector '%s': %s", collector_class, e)
        return ret

    def collect(self):
        """
        Collect a set of statistics by invoking all defined collectors and
        merging the data into one dictionary and pushing it onto the deque of
        historical statistics.  Maintain a history length as specified in the
        config file.

        Note: Priority is given to collectors based on the order that they are
        listed in the config file (ie. if two collectors produce the same
        statistic only the value produced by the *last* collector will be saved).
        Return: The dictionary of collected statistics
        """
        data = {}
        collect_start = time.time()
        for c in self.collectors:
            try:
                dict_recursive_update(data, c.collect())
            except Exception as e:
                if self.should_run:
                    self.logger.exception("Collection %s error: %s", c.__class__.__name__, e)
        collect_end = time.time()
        with self.data_lock:
            self.statistics.append(data)
            if len(self.statistics) > self.hist_len:
                self.statistics.popleft()

        self.data_logger.append_data(data, collect_start, collect_end)

        return data

    def interrogate(self):
        """ Take a snapshot of this Monitor object and return an Entity object. """
        if not self.is_ready:
            self.logger.warning("Not ready yet for interrogation")
            return None
        ret = MonitorDataEntity(monitor=self)
        with self.data_lock:
            ret.update_properties(self.properties)
            ret.update_variable(self.variables)
            ret.set_statistics(self.statistics)
        return ret

    def update_variables(self, e=None, **f):
        """ Update the variables array to store any updates from an Entity """
        with self.data_lock:
            self.variables.update(e, **f)

    def _set_ready(self):
        if not self.is_ready:
            self.logger.info('Ready')
        self.ready.set()

    def _set_not_ready(self, message=None):
        self.ready.clear()
        self.logger.error(message)

    @property
    def is_ready(self):
        return self.ready.is_set()


class EntityError(Exception):
    def __init__(self, message):
        self.message = message


class MonitorDataEntity:
    """
    An entity is an object that is designed to be inserted into the rule-
    processing namespace.  The properties and statistics elements allow it to
    contain a snapshot of Monitor data that can be used as inputs to rules.  The
    rule-accessible methods provide a simple syntax for referencing data.
    """
    __slots__ = ('monitor', 'properties', 'variables', 'statistics', 'controls')

    def __init__(self, monitor=None):
        self.monitor = monitor
        self.properties = {}
        self.variables = {}
        self.statistics = []
        self.controls = {}

    def set_property(self, name, val):
        self.properties[name] = val

    def update_properties(self, *args, **kwargs):
        self.properties.update(*args, **kwargs)

    def set_variable(self, name, val):
        self.variables[name] = val

    def update_variable(self, e=None, **f):
        self.variables.update(e, **f)

    def set_statistics(self, stats):
        for row in stats:
            self.statistics.append(row)

    def store_variables(self):
        """ Pass rule-defined variables back to the Monitor for storage """
        if self.monitor is not None:
            self.monitor.update_variables(self.variables, last_control=self.controls)

    def prop(self, key, default_value=None):
        """ Get the value of a single property """
        return self.properties.get(key, default_value)

    def stat(self, key):
        """
        Get the most-recently recorded value of a statistic
        Returns None if no statistics are available
        """
        if len(self.statistics) > 0:
            return self.statistics[-1][key]
        else:
            return None

    def stat_avg(self, key):
        """ Calculate the average value of a statistic using all recent values """
        if len(self.statistics) == 0:
            raise EntityError(f"Statistic '{key}' not available")
        return sum(map(lambda x: x[key], self.statistics)) / len(self.statistics)

    def set_var(self, key, val):
        """
        Store a named value in this Entity.
        """
        self.variables[key] = val

    def update_vars(self, new_vars):
        """
        takes a dictionary of variables and values and updates them all.
        """
        self.variables.update(new_vars)

    def get_var(self, key, default_value=None):
        """
        Get the value of a potential variable in this instance.
        Returns None if the variable has not been defined.
        """
        return self.variables.get(key, default_value)

    def control(self, key, val):
        """ Set a control variable in this instance. """
        self.controls[key] = val

    def get_control(self, key, default_value=None):
        """
        Get the value of a control variable in this instance if it exists.
        Returns None if the control has not been set.
        """
        return self.controls.get(key, default_value)

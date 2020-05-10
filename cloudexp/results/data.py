"""
@editor: Liran Funaro <funaro@cs.technion.ac.il>
@author: Alex Nulman <anulman@cs.haifa.ac.il>
"""
import os
import shutil
import string
import sqlite3
import pandas as pd
from contextlib import closing
from typing import MutableMapping

import msgpack
import msgpack_numpy

from cloudexp.exp import DEFAULT_DATA_FILE_NAME

from mom.logged_object import LoggedObject

msgpack_numpy.patch()


class ExpData(LoggedObject):
    """
    parses an experiment file into an sql table,
    or set path to the correct database if this is a previously parsed experiment
    takes file path as a parameter
    """
    valid_chars = f"-_.() {string.ascii_letters}{string.digits}"

    def __init__(self, output_path, export_path="exports", log_name=None):
        LoggedObject.__init__(self, log_name)
        if not output_path:
            raise Exception("No data output path")

        if not os.path.isdir(output_path):
            raise Exception(f"Data output path '{output_path}' not found")

        file_path = os.path.join(output_path, DEFAULT_DATA_FILE_NAME)
        if not os.path.isfile(file_path):
            raise Exception(f"Data file '{file_path}' not found")

        self.__output_path = output_path
        self.__file_name = DEFAULT_DATA_FILE_NAME
        self.__data_file_path = file_path

        self.__export_path = os.path.join(self.__output_path, export_path)

        self.__name, _ext = os.path.splitext(self.__file_name)
        self.__sql_file_path = os.path.join(self.__output_path,  f'{self.__name}.sqlite3')

        self.__column_names = None
        reload = self.__check_reload__()

        if reload:
            self.logger.info("Generating DB from data log.")
            self.clear_db()
            self.analyze()
        self.generate_cols()
        self.create_exports_path()

    @property
    def column_names(self):
        return self.__column_names

    @property
    def file_name(self):
        return self.__file_name

    @property
    def path(self):
        return self.__output_path

    @property
    def name(self):
        return self.__name

    def export_file_path(self, *parameters, ext=None):
        """ Create a valid file name for a plot export """
        file_name_list = [self.name]
        file_name_list.extend(parameters)
        file_name = '-'.join(file_name_list)
        if ext:
            file_name = f"{file_name}.{ext}"
        file_name = ''.join(c for c in file_name if c in self.valid_chars)
        return os.path.join(self.__export_path, file_name)

    def create_exports_path(self):
        os.makedirs(self.__export_path, exist_ok=True)

    def db_connection(self):
        return closing(sqlite3.connect(self.__sql_file_path))

    def is_db_exits(self):
        return os.path.isfile(self.__sql_file_path)

    def get_attributes(self, *attr):
        with self.db_connection() as conn:
            return [conn.execute(f'select `{a}` from attributes').fetchone()[0] for a in attr]

    def __check_reload__(self):
        """
        Check if the data file exists.
        If it is or there is an error it will re analyze it
        """
        if not self.is_db_exits():
            return True

        data_file_stats = os.stat(self.__data_file_path)
        current_size = data_file_stats.st_size
        current_timestamp = data_file_stats.st_mtime

        try:
            archived_size, archived_timestamp = self.get_attributes('size', 'time')

            if archived_size < current_size:  # experiment fle grew since last time we scanned it
                self.logger.info('Data log grew. Reloading.')
                return True
            elif archived_size > current_size:  # experiment file shrunk, this should never happen
                self.logger.info('Data log shrunk. Reloading.')
                return True
            elif archived_size == current_size and current_timestamp > archived_timestamp:
                self.logger.info('Data log timestamp has change. Reloading.')
                return True
        except Exception as e:
            self.logger.exception("Failed to read existing data status: %s", e)
            return True

        return False

    def clear_db(self):
        try:
            if self.is_db_exits():
                os.remove(self.__sql_file_path)
        except Exception as e:
            self.logger.exception("Could not clear DB: %s", e)

        try:
            if os.path.exists(self.__export_path):
                shutil.rmtree(self.__export_path)
        except Exception as e:
            self.logger.exception("Could not clear export folder: %s", e)

    def generate_cols(self):
        """ Generates the column names """
        with self.db_connection() as conn:
            self.__column_names = pd.read_sql_query("select * from columns", conn)['0'].tolist()

    def get_distinct_values(self, col):
        """ Returns the distinct values of a particular column """
        with self.db_connection() as conn:
            return pd.read_sql_query("select distinct([{}]) from data where [{}] != '' ".format(col, col), conn)[
                col].tolist()

    @classmethod
    def flatten(cls, d: MutableMapping, sep='_', *, parent_key=None):
        """
        Flattens multilevel dicts
        Take from: http://stackoverflow.com/questions/6027558/flatten-nested-python-dictionaries-compressing-keys
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(cls.flatten(v, sep, parent_key=new_key))
            else:
                items.append((new_key, v))
        return items

    @classmethod
    def normalize_data(cls, data: MutableMapping):
        for entry, value in cls.flatten(data, sep=':'):
            if type(value) in {list, tuple}:
                value = msgpack.dumps(value)
            yield entry, value

    def iter_analyze_all_lines(self, start_pos):
        """ Read the data file and parse it into a iterator of dicts """
        with open(self.__data_file_path, 'rb') as f:
            f.seek(start_pos)
            unpacker = msgpack.Unpacker(f, raw=False)
            for data_line in unpacker:
                res = dict(self.normalize_data(data_line))
                if res:
                    yield res

    def analyze(self):
        """ Parse and analyzes the data file and creates an SQL db file with the data """
        frame = pd.DataFrame(self.iter_analyze_all_lines(0))
        frame.sort_values('sample_start', inplace=True)
        min_time = frame['sample_start'].min()
        frame['sample_start'] = frame['sample_start'].apply(lambda x: x - min_time)
        frame['sample_end'] = frame['sample_end'].apply(lambda x: x - min_time)
        data_file_stats = os.stat(self.__data_file_path)

        try:
            with self.db_connection() as conn:
                pd.DataFrame(frame.columns).to_sql('columns', conn, if_exists='replace', index=False)
                frame.to_sql('data', conn, if_exists='append', index=False)

                attribute_data = pd.DataFrame(
                    [{'size': data_file_stats.st_size, 'time': data_file_stats.st_mtime, 'base-time': min_time}])
                attribute_data.to_sql('attributes', conn, if_exists='replace', index=False)
        except Exception as e:
            self.logger.exception("Failed to write data to DB: %s", e)
            self.clear_db()

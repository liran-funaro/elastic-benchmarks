"""
This module give some functionality on any group of dicts

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
from timeit import itertools
from typing import Iterable, Dict, Hashable, Any, List, Tuple, Iterator, Optional, Callable, Union


def unique_property(dict_iterable: Iterable[Dict], key: Hashable, default_value: Any = None) -> List[Any]:
    """
    Fetch a unique values of an attribute for all the dicts in an iterable
    :param dict_iterable: An iterable of dicts
    :param key: A attribute that the dicts contains
    :param default_value: The default value of the attribute if the dict does not contain it
    :return: A list of uniques values for this attribute for all the dics
    """
    return sorted({d.get(key, default_value) for d in dict_iterable})


def sorted_dicts(dict_iterable: Iterable[Dict], key: Hashable, default_value: Any = None) -> List[Any]:
    """
    Sort an iterable of dicts by a dict attribute
    :param dict_iterable: An iterable of dicts
    :param key: A attribute that the dicts contains
    :param default_value: The default value of the attribute if the dict does not contain it
    :return: A sorted list
    """
    return sorted(dict_iterable, key=lambda x: x.get(key, default_value))


def groupby_iterator(dict_iterable: Iterable[Dict], key: Hashable,
                     default_value: Any = None) -> Iterator[Tuple[Hashable, Iterable]]:
    """
    Group an iterable of dicts by a dict attribute
    :param dict_iterable: An iterable of dicts
    :param key: A attribute that the dicts contains
    :param default_value: The default value of the attribute if the dict does not contain it
    :return: Iterator for a tuple(key, iterable) where the key is the group common value
            and the iterable iterates over the dicts that had this value
    """
    return itertools.groupby(sorted_dicts(dict_iterable, key), lambda x: x.get(key, default_value))


def groupby(dict_iterable: Iterable[Dict], key: Hashable, id_key: Optional[Hashable] = None,
            list_func: Callable[[Iterable[Dict]], Iterable[Dict]] = tuple,
            default_value: Any = None) -> Dict[Hashable, Iterable]:
    """
    Covert the groupby_iterator() to a dict of tuples
    :param dict_iterable: An iterable of dicts
    :param key: A attribute that the dicts contains
    :param id_key: A key that is unique for each dict.
                If this parameter is not None, then instead of the dict itself, this
                value will be listed.
    :param list_func: A function that convert the iterable to a list/tuple/etc...
    :param default_value: The default value of the attribute if the dict does not contain it
    :return: A dict where the key is the group common value
            and the value is a tuple of the dicts that had this value.
    """
    if id_key is not None:
        user_list_func = list_func

        def list_func(i: Iterable[Dict]) -> Iterable[Dict]:
            return user_list_func(unique_property(i, id_key, default_value))

    groups = groupby_iterator(dict_iterable, key, default_value)
    return {k: list_func(i) for k, i in groups}


def nested_groupby(dict_iterable: Iterable[Dict], *key: List[Hashable], id_key: Optional[Hashable] = None,
                   list_func: Callable[[Iterable[Dict]], Iterable[Dict]] = tuple,
                   default_value: Any = None) -> Union[Iterable[Dict], Dict[Hashable, Iterable]]:
    """
    Group an iterable of dicts by multiple dict attributes
    :param dict_iterable: An iterable of dicts
    :param key: A list of attribute that the dicts contains
    :param id_key: A key that is unique for each dict.
                If this parameter is not None, then instead of the dict itself, this
                value will be listed.
    :param list_func: A function that convert the iterable to a list/tuple/etc...
    :param default_value: The default value of the attribute if the dict does not contain it
    :return: A recursive dict where the key is the group common value
            and the value is one of the following:
            1) A tuple of the dicts that had this value.
            2) A group dict of the next attribute
    """
    if len(key) == 0:
        if id_key is not None:
            dict_iterable = unique_property(dict_iterable, id_key, default_value)
        return list_func(dict_iterable)

    groups = groupby_iterator(dict_iterable, key[0], default_value)

    return {k: nested_groupby(i, *key[1:],
                              id_key=id_key,
                              list_func=list_func,
                              default_value=default_value) for k, i in groups}


def dict_recursive_update(source: Dict, input_dict: Dict):
    """
    Update a dict recursively:
    :param source: The dict to update
    :param input_dict: The dict to update from
    :return: None
    """
    for k, v in input_dict.items():
        source_v = source.get(k, None)
        if isinstance(source_v, dict) and isinstance(v, dict):
            dict_recursive_update(source_v, v)
        else:
            source[k] = v

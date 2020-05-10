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
from typing import Union, List
from xml.etree import ElementTree

XmlType = Union[str, ElementTree.Element]


def get_xml(xml: XmlType) -> ElementTree.Element:
    """
    Return XML Tree instance from string the instance itself
    :param xml: An XML string or ElementTree instance
    :return: ElementTree instance
    """
    if isinstance(xml, str):
        return ElementTree.fromstring(xml)
    else:
        return xml


def findall(xml: XmlType, xpath_query: str) -> List[ElementTree.Element]:
    """
    Find all results that match a xpath query
    :param xml: An XML string or ElementTree instance
    :param xpath_query: A query to find
    :return: All matching elements
    """
    xml = get_xml(xml)
    return xml.findall(xpath_query)


def get_element_text(xml: XmlType, xpath_query: str) -> str:
    """
    Get a text of an element
    :param xml: An XML string or ElementTree instance
    :param xpath_query: A query to find
    :return: The text of the first element to match the query
    """
    elements = findall(xml, xpath_query)
    if len(elements) > 0:
        return elements[0].text
    else:
        raise KeyError("Can not find element: %s" % xpath_query)


def find_attribute(xml: XmlType, xpath_query: str, attr: str) -> str:
    """
    Find specific attribute in all the elements that matches the query
    :param xml: An XML string or ElementTree instance
    :param xpath_query: A query to find
    :param attr: An attribute name to find
    :return: The first attribute value that matches or raise KeyError
    """
    elements = findall(xml, xpath_query)

    for elem in elements:
        if attr in elem.attrib:
            return elem.attrib[attr]

    raise KeyError("Can not find element and/or attribute: %s (%s)" % (xpath_query, attr))


def get_element(xml: XmlType, element_path: str, auto_built: bool = False) -> ElementTree.Element:
    """
    Get an element from XML
    :param xml: An XML string or ElementTree instance
    :param element_path: An XML path
    :param auto_built: If True, build the path in the XML if no exist
    :return: The element
    """
    xml = get_xml(xml)
    element = xml.find(element_path)
    if element is not None or not auto_built:
        return element

    sub_elements = element_path.split("/")
    if len(sub_elements) > 1:
        parent = get_element(xml, "/".join(sub_elements[:-1]), auto_built)
    else:
        parent = xml

    build_element = sub_elements[-1]
    if build_element == "":
        raise ValueError("Cannot build empty element")

    element = ElementTree.SubElement(parent, sub_elements[-1])
    parent.insert(0, element)
    return element


def get_updated_xml(xml: XmlType, element_path: str, text: str, **attributes) -> XmlType:
    """
    Update XML
    :param xml: An XML string or ElementTree instance
    :param element_path: An XML path
    :param text: The text to set at the given path
    :param attributes: The attributes to set at the given path
    :return: A new XML string if given a string or the modified XML ElementTree otherwise
    """
    keep_string = isinstance(xml, str)
    xml = get_xml(xml)
    element = get_element(xml, element_path, True)

    for attr, value in attributes:
        if value is None:
            if attr in element.attrib:
                del element.attrib[attr]
        else:
            element.attrib[attr] = value

    element.text = str(text)

    if keep_string:
        return ElementTree.tostring(xml)
    else:
        return xml


def to_string(xml: XmlType) -> str:
    if isinstance(xml, str):
        return xml
    else:
        return ElementTree.tostring(xml, encoding='unicode')

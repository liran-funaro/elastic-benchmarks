#! /usr/bin/env python
'''
Copyright 2014 Liran Funaro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author Liran Funaro <fonaro@cs.technion.ac.il>

Usage:
index.py [<directory>, <path_name>]

This will create an index.html file for every folder in the subtree of the given
folder (or the current folder if not specified).
If an info file exist (should be json/dict), it will be visible in the folder's
page.

NOTICE: This script also makes sure all of the files are visible to the public.
(i.e. "chmod 755 <file>" for all the files)
'''

import sys
import os
import stat
import re
import xml.etree.ElementTree as et
import fnmatch
from xml.sax.saxutils import escape  # To quote out things like &amp;
import urllib.request, urllib.parse, urllib.error
import datetime

class FolderIndex:
    ################################################################################
    # Constants
    ################################################################################
    TEMPLATE_XML_FILENAME = os.path.join(os.path.dirname(os.path.realpath(__file__)), "index_template.xml")
    INDEX_FILENAME = "index.html"
    INFO_FILENAME = "info"

    PUBLIC_FILE_MODE = 0o755

    # Find all "..." or '...': ^([^\'"]*(?:[^\'"]*(?:(?:\'[^\']*\')|(?:"[^"]"))[^\'"]*)*)'
    DICT_OBJ_PATTERN = re.compile('<([^<>]*)>')
    QUOTES_PATTERN = re.compile('\'|"')
    ICONS_PATH = "http://cs.technion.ac.il/~fonaro/icons"
    FOLDER_ICON = "folder.png"
    FOLDER_RETURN_ICON = "ret.gif"
    FILE_ICON = "file.png"
    TYPES_ICON = {
        ".pdf": "pdf.png",
        ".ini": "conf.png",
        ".png": "img.png",
        ".jpeg": "img.png",
        ".gif": "img.png",
        ".xml" : "xml.png",
        }
    ################################################################################

    def __init__(self, include = ["*"], exlude_index = True):
        accept_regex_array = [fnmatch.translate(a) for a in include]
        self.accept_regex = re.compile( "|".join(accept_regex_array) )

        self.exlude_index = exlude_index

        self.main_index_html_template, self.tablerow_templates, self.info_key_value_template = self.getTemplates()

    ################################################################################
    # Templates
    ################################################################################
    def innerNodeToString(self, node):
        return (node.text or '') + ''.join(map(et.tostring, node)) + (node.tail or '')

    def getTemplates(self):
        with open(self.TEMPLATE_XML_FILENAME, 'r') as f:
            template_xml = et.parse(f)

        main_template = self.innerNodeToString(template_xml.findall(".//main_html")[0])
        info_key_value_template = self.innerNodeToString(template_xml.findall(".//info_key_value")[0])

        tablerow_templates = {}

        for t in template_xml.findall(".//tablerow"):
            tablerow_templates[t.attrib["type"]] = dict(
                data=self.innerNodeToString(t),
                pattern=re.compile(t.attrib["pattern"]) if "pattern" in t.attrib else None
            )

        return main_template, tablerow_templates, info_key_value_template
    ################################################################################

    def getIconFile(self, filename, isdir):
        if isdir:
            if filename == "..":
                return self.FOLDER_RETURN_ICON
            else:
                return self.FOLDER_ICON

        ext = os.path.splitext(filename)[-1]

        if ext in self.TYPES_ICON:
            return self.TYPES_ICON[ext]
        else:
            return self.FILE_ICON

    def getMatchingTablerowTamplate(self, name):
        for temp in self.tablerow_templates:
            if "pattern" not in self.tablerow_templates[temp] or not self.tablerow_templates[temp]["pattern"]:
                continue

            if self.tablerow_templates[temp]["pattern"].match(name):
                return self.tablerow_templates[temp]["data"]

        return self.tablerow_templates["link"]["data"]

    def setPublicFile(self, filepath):
        try: os.chmod(filepath, self. PUBLIC_FILE_MODE);
        except: pass

    def isDir(self, path):
        try:
            return os.path.isdir(path)
        except:
            try:
                st = os.lstat(path)
                return stat.S_ISDIR(st.st_mode)
            except:
                return False

    def getChildren(self, path):
        yield "..", True, 0

        children = os.listdir(path)

        if self.INFO_FILENAME in children:
            yield self.INFO_FILENAME, False, os.path.getmtime(os.path.join(path, self.INFO_FILENAME))

        # add path to each file
        children = [ (f, os.path.join(path, f)) for f in children]
        children.sort(key=lambda x: (not self.isDir(x[1]), x[0])) # old key: -os.path.getmtime(x[1])

        for name, child_path in children:
            if self.exlude_index and name == self.INDEX_FILENAME:
                continue

            if name == self.INFO_FILENAME:
                continue

            isdir = self.isDir(child_path)

            if not isdir and not self.accept_regex.match(name):
                continue

            self.setPublicFile(child_path)
            yield name, isdir, os.path.getmtime(child_path)

    def getTableRows(self, folder_name, path):
        ret = []
        changed = []

        for name, isdir, last_modified in self.getChildren(path):
            template = self.getMatchingTablerowTamplate(name)

            if not isdir:
                last_modified = "Last Modified: %s" % datetime.datetime.fromtimestamp(last_modified).strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_modified = ""
            ret.append(template % dict(
    	            type="directory" if isdir else "",
                    name=escape(name),
    	            linkname=urllib.parse.quote(name, ""),
                    last_modified=last_modified,
    	            icons_path=self.ICONS_PATH,
    	            icon=self.getIconFile(name, isdir) ) )

            if isdir and name != "..":
                changed_sub_folders = self.writeIndexFile(os.path.join(folder_name, name), os.path.join(path, name))
                changed.extend( changed_sub_folders )

        return '\n'.join(ret), changed

    def evalInfo(self, info):
            '''
            Add quotes around objects from info text. Something like "< Bla Bla >"
            '''

#             def replaceIt(m):
#                 return "".join(["'\xEE", self.QUOTES_PATTERN.sub("", m.group(1)), "\xEF'"])
#
#             count = -1
#             while count != 0:
#                 info, count = self.DICT_OBJ_PATTERN.subn(replaceIt, info)
#
#             info = info.replace("\xEE", "<").replace("\xEF", ">")

            try:
                return eval(info)
            except Exception as e:
                return {"indexing-error": str(e)}


    def infoToHtml(self, info_key, info):
        return self.infoToHtmlWithTemplate(info_key, info, self.info_key_value_template)

    @classmethod
    def infoToHtmlWithTemplate(cls, info_key, info, template):
        if isinstance(info, dict):
            rows = []
            rows.append("{")

            for key, value in iter(sorted(info.items())):
                rows.append(cls.infoToHtmlWithTemplate(key, value, template))

            rows.append("}")

            value = "\n".join(rows)
        elif isinstance(info, list) or isinstance(info, tuple):
            rows = []
            rows.append("[" if isinstance(info, list) else "(")

            items_value = []
            for value in info:
                items_value.append( cls.infoToHtmlWithTemplate(None, value, template) )

            rows.append(", ".join(items_value))

            rows.append("]" if isinstance(info, list) else ")")

            value = "".join(rows)
        else:
            value = escape( str(info) )

        return template % dict(
                    key = escape( str(info_key) ),
                    value = value ) if info_key != None else value

    def getFolderInformation(self, path):
        info_path = os.path.join(path, self.INFO_FILENAME)

        if not os.path.isfile(info_path):
            return ""

        with open(info_path, "r") as f_info:
            info_raw = f_info.read()

        info_dict = self.evalInfo(info_raw)
        err = info_dict.get("indexing-error", None)
        if err:
            print("Failed retrieving information for folder: %s -- with: %s" % (path, err))

        return self.infoToHtml(None, info_dict)


    def getIndexHtml(self, folder_name, path):
        parents_folder = folder_name.split("/")
        max_depth = len(parents_folder) - 1

        links = [ '<a href="%s">%s</a>' % ("." if i==max_depth else "/".join([".."]*(max_depth-i)) ,folder) for i, folder in enumerate(parents_folder)]
        title_path = "/".join(links)

        tablerows, changed = self.getTableRows(folder_name, path)
        return self.main_index_html_template % dict(
            title_head=parents_folder[-1],
            title_path=title_path,
            tablerows=tablerows,
            info=self.getFolderInformation(path)
            ), changed

    def writeIndexFile(self, folder_name, path):
        output_path = os.path.join(path, self.INDEX_FILENAME)
        output_data, changed = self.getIndexHtml(folder_name, path)

        try:
            with open(output_path, "r") as f:
                old_data = f.read()
        except:
            old_data = ""

        if old_data != output_data:
            with open(output_path, "w+") as f:
                f.write(output_data)

            changed = [ (folder_name, path) ]

        self.setPublicFile(output_path)

        return changed

if __name__ == '__main__':
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = os.getcwd()

    if len(sys.argv) > 2:
        start_folder_name = sys.argv[2]
    else:
        start_folder_path, start_folder_name = os.path.split(path)

    FolderIndex().writeIndexFile(start_folder_name, path)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base
import string
import os
import shutil

try:
    import yaml
except:
    from brewery.utils import MissingPackage
    yaml = MissingPackage("PyYAML", "YAML directory data source/target", "http://pyyaml.org/")

class YamlDirectoryDataSource(base.DataSource):
    """docstring for ClassName
    """
    def __init__(self, path, extension="yml", expand=False, filename_field=None):
        """Creates a YAML directory data source stream.
        
        The data source reads files from a directory and treats each file as single record. For example,
        following directory will contain 3 records::
        
            data/
                contract_0.yml
                contract_1.yml
                contract_2.yml
        
        Optionally one can specify a field where file name will be stored.
        
        
        :Attributes:
            * path: directory with YAML files
            * extension: file extension to look for, default is ``yml``,if none is given, then
              all regular files in the directory are read
            * expand: expand dictionary values and treat children as top-level keys with dot '.'
                separated key path to the child.. Default: False
            * filename_field: if present, then filename is streamed in a field with given name,
              or if record is requested, then filename will be in first field.
        
        """
        self.path = path
        self.expand = expand
        self.filename_field = filename_field
        self.extension = extension

    def initialize(self):
        pass

    def records(self):
        files = os.listdir(self.path)

        for base_name in files:
            split = os.path.splitext(base_name)
            if split[1] != self.extension:
                pass

            # Read yaml file
            handle = open(os.path.join(self.path, base_name), "r")
            record = yaml.load(handle)
            handle.close()

            # Include filename in output record if requested
            if self.filename_field:
                record[self.filename_field] = base_name

            yield record

    def rows(self):
        if not self.field_names:
            raise Exception("Field names not initialized, can not generate rows")

        for record in self.records():
            row = []
            for field in self.field_names:
                row.append(record.get(field))
            yield row


class YamlDirectoryDataTarget(base.DataTarget):
    """docstring for YamlDirectoryDataTarget
    """
    def __init__(self, path, filename_template="record_${__index}.yml", expand=False,
                    filename_start_index=0, truncate=False):
        """Creates a directory data target with YAML files as records.
        
        :Attributes:
            * path: directory with YAML files
            * extension: file extension to use
            * expand: expand dictionary values and treat children as top-level keys with dot '.'
              separated key path to the child.. Default: False
            * filename_template: template string used for creating file names. ``${key}`` is replaced
              with record value for ``key``. ``__index`` is used for auto-generated file index from
              `filename_start_index`. Default filename template is ``record_${__index}.yml`` which
              results in filenames ``record_0.yml``, ``record_1.yml``, ...
            * filename_start_index - first value of ``__index`` filename template value, by default 0
            * filename_field: if present, then filename is taken from that field.
            * truncate: remove all existing files in the directory. Default is ``False``.

        """

        self.filename_template = filename_template
        self.filename_start_index = filename_start_index
        self.path = path
        self.expand = expand
        self.truncate = truncate

    def initialize(self):
        self.template = string.Template(self.filename_template)
        self.index = self.filename_start_index

        if os.path.exists(self.path):
            if not os.path.isdir(self.path):
                raise Exception("Path %s is not a directory" % self.path)
            elif self.truncate:
                shutil.rmtree(self.path)
                os.makedirs(self.path)
        else:
            os.makedirs(self.path)


    def append(self, obj):

        if type(obj) == dict:
            record = obj
        else:
            record = dict(zip(self.field_names, obj))

        base_name = self.template.substitute(__index=self.index, **record)
        path = os.path.join(self.path, base_name)

        handle = open(path, "w")
        yaml.safe_dump(record, stream=handle, encoding=None, default_flow_style=False)
        handle.close()

        self.index += 1

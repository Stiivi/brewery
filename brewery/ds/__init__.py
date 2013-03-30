#!/usr/bin/env python
# -*- coding: utf-8 -*-

from base import *
from brewery.ds.gdocs_streams import *
from brewery.ds.mongo_streams import *
from brewery.ds.elasticsearch_streams import *
from brewery.ds.stream_auditor import *
from brewery.ds.yaml_dir_streams import *
from brewery.ds.html_target import *

# Backward Compatibility
from ..backends.text import CSVDataSource, CSVDataTarget
from ..backends.sql import SQLTable
from ..backends.xls import *

# FIXME: backward compatibility
SQLDataSource = SQLTable
SQLDataTarget = SQLTable

__all__ = (
    "Field",
    "FieldList",

    "DataStream",
    "DataSource",
    "DataTarget",

    "XLSDataSource",
    "MongoDBDataSource",
    "ESDataSource",
    "GoogleSpreadsheetDataSource",
    "YamlDirectoryDataSource",
    "YamlDirectoryDataTarget",
    "StreamAuditor",
    "SimpleHTMLDataTarget",

    # Moved to stores
    "CSVDataSource",
    "CSVDataTarget",
    "SQLDataSource",
    "SQLDataTarget",

)

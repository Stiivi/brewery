#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Brewery"""

from metadata import *

__version__ = '0.8.0'

def default_logger_name():
    return 'brewery'

__all__ = (
    "Field",
    "FieldList",
    "fieldlist",
    "expand_record",
    "collapse_record",
    "default_logger_name",
    "set_brewery_search_paths",
    "__version__"
)
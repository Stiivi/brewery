#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Brewery"""

from metadata import *
from streams import *

__version__ = '0.8.0'

def default_logger_name():
    return 'brewery'

__all__ = [
    "default_logger_name",
    "set_brewery_search_paths",
    "__version__"
]

__all__ += metadata.__all__
__all__ += streams.__all__

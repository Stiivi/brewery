# -*- coding: utf-8 -*-
"""
    Brewery
    ~~~~~~~
    
    Framework for stream-based data analysis and auditing. Focuses on
    understandability and auditability of the analytical proces.

    :license: MIT, see LICENSE for more details
"""

__version__ = '0.8.0'

from metadata import *
from streams import *
from utils import *

__all__ = [
    "logger_name",
    "get_logger",
    "set_brewery_search_paths",
    "__version__"
]

__all__ += metadata.__all__
__all__ += streams.__all__

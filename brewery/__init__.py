"""Brewery"""

import os
import sys

import brewery.ds
import brewery.dq
import brewery.utils
import brewery.streams
import brewery.nodes

try:
    import json
except ImportError:
    import simplejson as json

__version__ = '0.6.0'

brewery_search_paths = ['/etc/brewery', \
						'~/.brewery/', \
						'./.brewery/']

def set_brewery_search_paths(paths):
	global brewery_search_paths
	brewery_search_paths = paths

def default_logger_name():
    return 'brewery'

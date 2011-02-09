"""Brewery"""

import os
import sys

import brewery.ds
import brewery.dq
import brewery.utils

try:
    import json
except ImportError:
    import simplejson as json

__version__ = '0.5.0'

brewery_search_paths = ['/etc/brewery', \
						'~/.brewery/', \
						'./.brewery/']

def set_brewery_search_paths(paths):
	global brewery_search_paths
	brewery_search_paths = paths

def default_logger_name():
    return 'brewery'

def split_field(field):
    """Split field reference.
    
    Example: "fact.amount" will be split into ("fact", "amount")
    
    Args:
        field: field reference to be split
        
    Return:
        tuple of field parts"""
        
    return field.split('.')
    
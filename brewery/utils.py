#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Brewery handy utilities"""

import re
import logging

logger_name = 'brewery'
logger = None

def get_logger():
    """Get brewery default logger"""
    global logger
    
    if logger:
        return logger
    else:
        return create_logger()
        
def create_logger():
    """Create a default logger"""
    global logger
    logger = logging.getLogger(logger_name)

    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s')
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

class MissingPackage(object):
    """Bogus class to handle missing optional packages - packages that are not necessarily required
    for brewery, but are needed for certain features."""
    
    def __init__(self, package, feature = None, source = None, comment = None):
        self.package = package
        self.feature = feature
        self.source = source
        self.comment = comment

    def __getattr__(self, name):
        if self.feature:
            use = " to be able to use: %s" % self.feature
        else:
            use = ""
            
        if self.source:
            source = " from %s" % self.source
        else:
            source = ""
            
        if self.comment:
            comment = ". %s" % self.comment
        else:
            comment = ""

        raise Exception("Optional package '%s' is not installed. Please install the package%s%s%s" % 
                            (self.package, source, use, comment))

class IgnoringDictionary(dict):
    """Simple dictionary extension that will ignore any keys of which values are empty (None/False)"""
    def setnoempty(self, key, value):
        """Set value in a dictionary if value is not null"""
        if value:
            self[key] = value

def subclass_iterator(cls, _seen=None):
    """
    Generator over all subclasses of a given class, in depth first order.

    Source: http://code.activestate.com/recipes/576949-find-all-subclasses-of-a-given-class/
    """

    if not isinstance(cls, type):
        raise TypeError('_subclass_iterator must be called with '
                        'new-style classes, not %.100r' % cls)
    if _seen is None: _seen = set()
    try:
        subs = cls.__subclasses__()
    except TypeError: # fails only when cls is type
        subs = cls.__subclasses__(cls)
    for sub in subs:
        if sub not in _seen:
            _seen.add(sub)
            yield sub
            for sub in subclass_iterator(sub, _seen):
                yield sub

def decamelize(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)

def to_identifier(name):
    return re.sub(r' ', r'_', name).lower()
    


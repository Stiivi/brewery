"""Brewery handy utilities"""

import re

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

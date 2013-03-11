# -*- coding: utf-8 -*-
# Language utility functions.
#
# For data and management functions see common.py

def generative(fn):
    """Mark a method as generative - method that returns a modified copy of
    self.  """

    # Modified code from SQLAlchemy

    @wraps(fn)
    def decorator(*args, **kw):
        # Get object's self
        self = args[0]
        # Create 'new self' and copy instance variables of the original
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        fn(s, *args[1:], **kw)
        return s
    return decorator

def experimental(fn):
    """Mark a pethod as experimental. Interface or implementation of an
    experimental method might very likely change. Use with caution. This
    decorator just appends a doc string."""

    warning = \
    """
    .. warning::

       This method is experimental. Interface or implementation might
       change. Use with caution and note that you might have to modify
       your code later.
    """

    if fn.__doc__ is not None:
        fn.__doc__ += warning
    fn._brewery_experimental = True

    return fn

def required(fn):
    """Mark method as required to be implemented by scubclasses"""
    fn._brewery_required = True
    return fn

def recommended(fn):
    """Mark method as recommended to be implemented by subclasses"""
    fn._brewery_recommended = True
    return fn


# -*- Encoding: utf8 -*-
"""DataObject operations"""
from collections import defaultdict, namedtuple
# Inspired by sqlalchemy.sql.expression._FunctionGenerator from SQLAlchemy


Operation = namedtuple("Operation", ["name", "func", "signature"])

class OperationMap(object):
    def __init__(self, operations = None):
        self.operations = operations or []

    def add(self, o):
        """Adds an operation"""
        print "Adding operation %s(%s) as %s" % (o.name, o.signature, o.func)
        self.operations.append(o)

    def match(self, *args, **kwargs):
        """Returns a matching function for given arguments"""
        pass

_default_map = OperationMap()

def signature(*signature, **kwargs):
    """Marks a function as an operation and registers it."""
    def decorator(fn):
        options = dict(kwargs)
        name = options.pop("name", fn.__name__)
        operation = Operation(name, fn, signature)
        _default_map.add(operation)
        return fn
    return decorator


class _OperationApplicator(object):
    def __init__(self, **opts):
        self.__names = []
        self.opts = opts

    def __getattr__(self, name):
        # passthru __ attributes; fixes pydoc
        if name.startswith('__'):
            try:
                return self.__dict__[name]
            except KeyError:
                raise AttributeError(name)

        elif name.endswith('_'):
            name = name[0:-1]
        f = _FunctionGenerator(**self.opts)
        f.__names = list(self.__names) + [name]
        return f

    def __call__(self, *c, **kwargs):
        o = self.opts.copy()
        o.update(kwargs)

        tokens = len(self.__names)

        if tokens == 2:
            package, fname = self.__names
        elif tokens == 1:
            package, fname = "_default", self.__names[0]
        else:
            package = None

        if package is not None and \
            package in functions._registry and \
            fname in functions._registry[package]:
            func = functions._registry[package][fname]
            return func(*c, **o)

        return Function(self.__names[-1],
                        packagenames=self.__names[0:-1], *c, **o)

op = _OperationApplicator()

class _FuncionGenerator(object):
    """Generate :class:`.Function` objects based on getattr calls."""

    def __init__(self, **opts):
        self.__names = []
        self.opts = opts

    def __getattr__(self, name):
        # passthru __ attributes; fixes pydoc
        if name.startswith('__'):
            try:
                return self.__dict__[name]
            except KeyError:
                raise AttributeError(name)

        elif name.endswith('_'):
            name = name[0:-1]
        f = _FunctionGenerator(**self.opts)
        f.__names = list(self.__names) + [name]
        return f

    def __call__(self, *c, **kwargs):
        o = self.opts.copy()
        o.update(kwargs)

        tokens = len(self.__names)

        if tokens == 2:
            package, fname = self.__names
        elif tokens == 1:
            package, fname = "_default", self.__names[0]
        else:
            package = None

        if package is not None and \
            package in functions._registry and \
            fname in functions._registry[package]:
            func = functions._registry[package][fname]
            return func(*c, **o)

        return Function(self.__names[-1],
                        packagenames=self.__names[0:-1], *c, **o)

# "func" global - i.e. func.count()
# func = _FunctionGenerator()


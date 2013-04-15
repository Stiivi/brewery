# -*- Encoding: utf8 -*-
"""DataObject operations"""
from .errors import *
from .objects import *
from collections import defaultdict, namedtuple
import itertools
# Inspired by sqlalchemy.sql.expression._FunctionGenerator from SQLAlchemy

__all__ = (
            "common_representations",
            "signature_match",
            "operation",
            "match_operation",
            "extract_representations",
            "remove_operation",
            "Signature",
            "OperationMap"
        )


Operation = namedtuple("Operation", ["name", "func", "signature"])

"""Representations of an argument"""
ArgRepresentation = namedtuple("ArgRepresentation", ["reprs", "is_list"])

class Signature(object):
    def __init__(self, *signature):
        """Creates an operation signature"""
        self.signature = tuple(signature)

    def __getitem__(self, index):
        return self.signature[index]

    def __len__(self):
        return len(self.signature)

    def __eq__(self, obj):
        """Signatures can be compared to lists or tuples of strings"""

        if isinstance(obj, Signature):
            return self.signature == obj.signature
        elif isinstance(obj, (list, tuple)):
            return self.signature == tuple(obj)
        else:
            return False

    def __ne__(self, obj):
        return not self.__eq__(obj)

    def matches(self, *arguments):
        """returns `True` if the signature matches representations of
        `arguments` which is list of extracted representations from objects.
        Otherwise returns `False`.

        Use as: `signature.matches(extracted_representations(*objects))`.
        """

        if len(arguments) != len(self.signature):
            return False

        for sig, arg in zip(self.signature, arguments):
            if not signature_match(sig, arg.reprs, arg.is_list):
                return False
        return True

    def __hash__(self):
        return hash(self.replist)


def signature_match(sig, reps, is_list=False):
    """ Determines whether representations `reps` match single argument
    signature `sig`.

    Rules:

    * "repr" matches only representation ``repr``
    * "*" matches any representation
    * "*[]" matches list of objects with any representation"
    * "repr[]" matches list of objects with representation ``repr``
    """

    if sig.endswith("[]"):
        sig_is_list = True
        sig = sig[0:-2]
    else:
        sig_is_list = False

    if sig_is_list != is_list:
        return False
    elif sig == "*":
        return True
    else:
        return (sig in reps)

def common_representations(*objects):
    """Return list of common representations of `objects`"""
    setlist = map(set, [o.representations() for o in objects])
    return list(set.intersection(*setlist))

def extract_representations(*objects):
    """Extract representations of object arguments. Returns a list of tuples.
    """
    representations = []
    # Get representations of objects
    for obj in objects:
        if isinstance(obj, DataObject):
            objsig = ArgRepresentation(obj.representations(), False)
            representations.append(objsig)
        elif isinstance(obj, (list, tuple)):
            common = common_representations(*obj)
            rep = ArgRepresentation(common, True)
            representations.append(rep)
        else:
            raise ArgumentError("Unknown type of operation argument "\
                                "%s" % type(obj))

    return representations

class OperationMap(object):
    def __init__(self):
        """Creates a map of operations and their signatures"""
        super(OperationMap, self).__init__()
        self.operations = defaultdict(list)

    def add(self, name, fn, sig):
        """Adds an operation"""
        o = Operation(name, fn, sig)
        print "Adding operation %s(%s) as %s" % (o.name, o.signature, o.func)
        self.operations[o.name].append(o)

    def remove(self, name, signature=None):
        """Removes all operations with `name` and `signature`. If no
        `signature` is specified, then all operations with given name are
        removed."""

        operations = self.operations.get(name)
        if not operations:
            return
        elif not signature:
            del self.operations[name]

        print "=== DELETING %s with %s" % (name, signature)
        print "--- has %s ops" % len(operations)
        newops = [op for op in operations if op.signature != signature]
        print "--- new %s ops" % len(newops)
        self.operations[name] = newops

    def match(self, name, *objlist):
        """Returns a matching function for given data objects as arguments.

        Note: If the match does not fit your expectations, it is recommended
        to pefrom explicit object conversion to an object with desired
        representation.


        Rules:
            * ...
        """

        # Get all signatures registered for the operation
        try:
            operations = self.operations[name]
        except KeyError:
            raise OperationError("Unknown operation '%s'" % name)

        if not operations:
            raise OperationError("No known signatures for operation '%s'" %
                                                                        name)

        representations = extract_representations(*objlist)
        print "--- REPS: %s" % (representations, )

        print "--- OPS: %s" % (operations, )
        match = None
        for op in operations:
            if op.signature.matches(*representations):
                match = op
                break

        if match:
            return match.func

        # FIXME: remove objlist
        raise OperationError("No matching signature found for operation '%s' "
                             " (representations: %s)" %
                                (name, representations))


def match_operation(name, *objlist):
    """Returns an operation from default map that matches `name` and
    `signature`"""
    return _default_map.match(name, *objlist)

def remove_operation(name, signature=None):
    _default_map.remove(name, signature)

_default_map = OperationMap()

def operation(*signature, **kwargs):
    """Marks a function as an operation and registers it."""
    def decorator(fn):
        options = dict(kwargs)
        name = options.pop("name", fn.__name__)

        sig = Signature(*signature)
        _default_map.add(name, fn, sig)
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


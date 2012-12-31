from functools import wraps
from collections import OrderedDict
import itertools
import functools

# FIXME: unused yet
def inputs(*reprs, **named_reprs):
    rep_dict = {}
    for i, rep in enumerate(reprs):
        rep_dict[i] = rep
    rep_dict.update(named_reprs)

    def decorate(f):
        @wraps(f)
        def wrapper(inputs, *args, **kwds):
            print 'Checking representation types'
            for name, obj in inputs.items():
                if name in rep_dict \
                        and rep_dict[name] not in obj.representations():
                    raise Exception("Incompatible representations %s for "
                                    "function %s" % (obj.representations(), f.__name__))
            return f(inputs, *args, **kwds)
        return wrapper
    return decorate

# FIXME: unused yet
def check_input_reprs(inputs, *reprs, **named_reprs):
    """Check whether `inputs` have desired representations. `inputs` should be
    a dictionary. Positional arguments in `reprs` are going to be referenced
    by their index.

    Raises `RepresentationError` when representations do not match.
    """
    rep_dict = {}
    for i, rep in enumerate(reprs):
        rep_dict[i] = rep
    rep_dict.update(named_reprs)

    for name, obj in inputs.items():
        if name in rep_dict \
                and rep_dict[name] not in obj.representations():
            raise RepresentationError("Incompatible representations %s for "
                            "function %s" % (obj.representations(), f.__name__))


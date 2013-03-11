# -*- coding: utf-8 -*-
"""Data Object composition methods"""

from .errors import *
from .metadata import *
from .objects.base import *
from . import ops
from .util import *

# Notes:
#
# * this module is supposed to contain optimization methods that will compose
#   object using their best representations
#
# ... not yet

@experimental
def left_inner_join(master, details, joins):
    """Returns an object representing left inner join of `master` object and
    respective `details` joined by keys specified in `joins`.

    `joins` is a list of tuples `(master, detail)` with fields for respective
    data objects.

    .. note::

        Current implementation returns iterator-based object and consumes all
        details.
    """

    # Distill joins
    distilled_joins = []
    for detail, join in zip(details, joins):
        master_key = join[0]
        detail_key = join[1]
        master_index = master.fields.index(master_key)
        detail_index = detail.fields.index(detail_key)
        distilled_joins.append( (master_index, detail_index) )

    # TODO: Proper way of doing this:
    # 1. check composability of objects
    # 2. use appropriate composing methods
    # 3. return object with the best representation possible

    # FIXME: temporary solution: use iterator object

    master_iterator = iter(master)
    det_iterators = [iter(detail) for detail in details]

    iterator = ops.iterator.left_inner_join(master_iterator,
                                            det_iterators,
                                            distilled_joins)

    fields = list(master.fields)
    for detail in details:
        fields += detail.fields
    fields = FieldList(fields)

    return IterableDataSource(iterator, fields)


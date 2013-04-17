import sqlalchemy
from sqlalchemy import sql
from ...operations import operation
import functools

__all__ = [
            "distinct",
            "distinct_rows",
            "append",
            "sample",
            "field_filter",
            "duplicates",
            "duplicate_stats",
            "sort"
        ]


def _unary(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        obj = args[0]
        args = args[1:]

        result = func(obj.sql_statement, *args, **kwargs)

        return obj.clone(statement=result)

    return decorator

@operation("sql")
@_unary
def distinct(statement, keys):
    """Returns a statement that selects distinct values for `keys`"""

    cols = [statement.c[str(key)] for key in keys]
    statement = sql.expression.select(cols, from_obj=statement, group_by=cols)
    return statement

@operation("sql")
@_unary
def distinct_rows(statement, keys):
    """Returns a statement that selects whole rows with distinct values for
    `keys`"""
    raise NotImplementedError
    cols = [statement.c[str(key)] for key in keys]
    distinct = statement.select(cols).group_by(cols)
    conditions = []
    for key in keys:
        key = str(key)
        conditions += statement.c[key] == distinct.c[key]
    full = distinct.join(statement)
    return statement

@operation("sql[]")
# FIXME: use objects
def append(statements):
    """Returns a statement with sequentialy concatenated results of the
    `statements`. Statements are chained using ``UNION``."""
    return sqlalchemy.sql.expression.union(*statements)

@operation("sql")
@_unary
def sample(statement, value, mode="first"):
    """Returns a sample. `statement` is expected to be ordered."""

    if mode == "first":
        return statement.limit(value)
    else:
        raise Exception("Unknown sample mode '%s'" % mode)

@operation("sql")
def field_filter(obj, field_filter):
    """Returns a statement with fields according to the field filter"""
    statement = obj.sql_statement()

    columns = []

    for field in obj.fields:
        name = str(field)
        column = statement.c[name]
        if name in field_filter.rename:
            column = column.label(field_filter.rename[name])
        columns.append(column)

    row_filter = field_filter.row_filter(fields)
    selection = row_filter(columns)

    statement = sql.expression.select(selection, from_obj=statement)
    fields = field_filter.filter(fields)

    result = obj.clone(statement=statement, fields=fields)
    return result


@operation("sql")
def duplicates(obj, keys=None, threshold=1,
               record_count_label="__record_count"):
    """Returns duplicate rows based on `keys`. `threshold` is lowest number of
    duplicates that has to be present to be returned. By default `threshold`
    is 1. If no keys are specified, then all columns are considered."""

    if not threshold or threshold < 1:
        raise ValueError("Threshold should be at least 1 "
                         "meaning 'at least one duplcate'.")

    statement = obj.sql_statement()

    if keys:
        group = [statement.c[str(field)] for field in keys]
        out_fields = FieldList(*keys)
    else:
        group = list(statement.columns)
        out_fields = obj.fields.clone()

    counter = sqlalchemy.func.count("*").label(record_count_label)
    selection = group + [counter]
    condition = counter > threshold

    statement = sql.expression.select(selection,
                                   from_obj=statement,
                                   group_by=group,
                                   having=condition)

    out = obj.clone(statement=statement, fields=out_fields)
    return result

@operation("sql")
@_unary
def sort(statement, orders):
    """Returns a ordered SQL statement. `orders` should be a list of
    two-element tuples `(field, order)`"""

    # Each attribute mentioned in the order should be present in the selection
    # or as some column from joined table. Here we get the list of already
    # selected columns and derived aggregates

    columns = []
    for field, order in orders:
        column = statement.c[str(field)]
        order = order.lower()
        if order.startswith("asc"):
            column = column.asc()
        elif order.startswith("desc"):
            column = column.desc()
        else:
            raise ValueError("Unknown order %s for column %s") % (order, column)

    columns.append(column)

    return statement.order_by(*columns)

@operation("sql", "sql[]")
def left_inner_join(master, details, joins):
    """Creates left inner master-detail join (star schema) where `master` is an
    iterator if the "bigger" table `details` are details. `joins` is a list of
    tuples `(master, detail)` where the master is index of master key and
    detail is index of detail key to be matched.

    If `inner` is `True` then inner join is performed. That means that only
    rows from master that have corresponding details are returned.

    .. warning::

        all detail iterators are consumed and result is held in memory. Do not
        use for large datasets.
    """

    if not details:
        raise ArgumentError("No details provided, nothing to join")

    if not joins:
        raise ArgumentError("No joins specified")

    if len(details) != len(joins):
        raise ArgumentError("For every detail there should be a join "
                            "(%d:%d)." % (len(details), len(joins)))

    if not all(master.can_compose(detail) for detail in details):
        raise RetryOperation("rows", "rows[]")

    out_fields = master.fields
    for detail in details:
        out_fields += detail.fields

    selection = list(master.columns())
    joined = master.sql_statement()

    for detail, join in zip(details, joins):
        selection += detail.columns()
        onclause = master.column(join[0]) == detail.column(join[1])

        joined = sql.expression.join(joined,
                                     detail.sql_statement(),
                                     onclause=onclause)

    select = sql.expression.select(selection,
                                from_obj=joined,
                                use_labels=True)

    return master.clone(statement=select)

@operation("sql_statement")
def duplicate_stats(obj, fields=None, threshold=1):
    """Return duplicate statistics of a `statement`"""
    count_label = "__record_count"
    dups = duplicates(obj, threshold, count_label)
    statement = dups.statement
    statement = statement.alias("duplicates")

    counter = sqlalchemy.func.count("*").label("record_count")
    group = statement.c[count_label]
    result_stat = sqlalchemy.sql.expression.select([counter, group],
                                              from_obj=statement,
                                              group_by=[group])

    fields = dups.fields.clone()
    fields.add(count_label)

    result = obj.clone(statement=result_stat, fields=fields)
    return result


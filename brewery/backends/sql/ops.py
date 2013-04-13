import sqlalchemy
from sqlalchemy import sql
from ...operations import signature

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

@signature("sql_statement")
def distinct(statement, keys):
    """Returns a statement that selects distinct values for `keys`"""

    cols = [statement.c[str(key)] for key in keys]
    statement = sql.expression.select(cols, from_obj=statement, group_by=cols)
    return statement

@signature("sql_statement")
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

@signature("sql_statement[]")
def append(statements):
    """Returns a statement with sequentialy concatenated results of the
    `statements`. Statements are chained using ``UNION``."""
    return sqlalchemy.sql.expression.union(*statements)

@signature("sql_statement")
def sample(statement, value, mode="first"):
    """Returns a sample. `statement` is expected to be ordered."""

    if mode == "first":
        return statement.limit(value)
    else:
        raise Exception("Unknown sample mode '%s'" % mode)

@signature("sql_statement")
def field_filter(statement, fields, field_filter):
    """Returns a statement with fields according to the field filter"""
    columns = []
    for field in fields:
        name = str(field)
        column = statement.c[name]
        if name in field_filter.rename:
            column = column.label(field_filter.rename[name])
        columns.append(column)

    row_filter = field_filter.row_filter(fields)
    selection = row_filter(columns)

    statement = sql.expression.select(selection, from_obj=statement)

    return statement


@signature("sql_statement")
def duplicates(statement, keys=None, threshold=1,
               record_count_label="__record_count"):
    """Returns a statement that selects duplicate rows based on `keys`.
    `threshold` is lowest number of duplicates that has to be present to be
    returned. By default `threshold` is 1. If no keys are specified, then all
    columns are considered."""

    if not threshold or threshold < 1:
        raise ValueError("Threshold should be at least 1 "
                         "meaning 'at least one duplcate'.")

    if keys:
        group = [statement.c[str(field)] for field in keys]
    else:
        group = list(statement.columns)

    counter = sqlalchemy.func.count("*").label(record_count_label)
    selection = group + [counter]
    condition = counter > threshold

    result = sql.expression.select(selection,
                                   from_obj=statement,
                                   group_by=group,
                                   having=condition)

    return result

@signature("sql_statement")
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

# TODO: make this brewery-level method on top of data object
@signature("sql_statement")
def duplicate_stats(statement, fields=None, threshold=1):
    """Return duplicate statistics of a `statement`"""
    count_label = "__record_count"
    dups = duplicates(statement, fields, threshold, count_label)
    dups = dups.alias("duplicates")

    counter = sqlalchemy.func.count("*").label("record_count")
    group = dups.c[count_label]
    result = sqlalchemy.sql.expression.select([counter, group], from_obj=dups, group_by=[group])
    return result


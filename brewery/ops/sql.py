import sqlalchemy
from sqlalchemy import sql

__all__ = [
            "distinct",
            "distinct_rows",
            "append",
            "sample"
        ]

"toto je text"

def distinct(statement, keys):
    """Returns a statement that selects distinct values for `keys`"""

    cols = [statement.c[str(key)] for key in keys]
    statement = sql.expression.select(cols, from_obj=statement, group_by=cols)
    return statement

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

def append(statements):
    """Returns a statement with sequentialy concatenated results of the
    `statements`. Statements are chained using ``UNION``."""
    return sqlalchemy.sql.expression.union(*statements)

def sample(statement, value, mode="first"):
    """Returns a sample. `statement` is expected to be ordered."""

    if mode == "first":
        return statement.limit(value)
    else:
        raise Exception("Unknown sample mode '%s'" % mode)

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

def unique(statement, keys):
    """Returns a statement that selects only unique rows for `keys`"""
    raise NotImplementedError



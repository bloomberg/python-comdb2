"""Factory functions for use with Comdb2.

Both `comdb2.dbapi2.Connection` and `comdb2.cdb2.Handle` have a public
`row_factory` property that can be used to control the type used for result
rows.  By default, rows are returned as lists, but you can receive rows as
namedtuples by using `namedtuple_row_factory` as a `row_factory`, and likewise
you can receive rows as dicts by using `dict_row_factory`.

A factory function will be called with a list of column names, and must return
a callable that will be called once per row with a list of column values.
"""
from collections import namedtuple


def namedtuple_row_factory(col_names):
    """Return result rows as namedtuples."""
    return namedtuple('Row', col_names)._make


def dict_row_factory(col_names):
    """Return result rows as dicts mapping column names to values."""
    distinct_col_names = set(col_names)
    if len(col_names) != len(distinct_col_names):
        for name in distinct_col_names:
            if col_names.count(name) > 1:
                raise ValueError("Multiple columns named '%s'" % name)
    def dict_row(col_vals):
        return dict(zip(col_names, col_vals))
    return dict_row

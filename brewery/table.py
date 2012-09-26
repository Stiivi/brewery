from collections import OrderedDict

class PythonTable(object):
    """Default table implementation with python objects."""
    def __init__(self, columns, fields):
        self.table = OrderedDict()
        for field, column in zip(fields, columns):
            self.table[str(field)] = column

    def column(self, name):
        return self.table[name]

    def rows(self):
        """Iterate through table rows."""
        cols = self.table.values()
        for row in zip(*cols):
            yield row

    def __iter__(self):
        cols = self.table.values()
        for row in zip(*cols):
            yield row

    def append(self, row):
        cols = self.table.values()
        for c, value in enumerate(row):
            cols[c].append(value)


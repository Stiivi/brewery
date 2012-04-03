#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base
import datetime
from brewery.metadata import FieldList

try:
    import xlrd
except:
    from brewery.utils import MissingPackage
    xlrd = MissingPackage("xlrd", "Reading MS Excel XLS Files", "http://pypi.python.org/pypi/xlrd")

class XLSDataSource(base.DataSource):
    """Reading Microsoft Excel XLS Files

    Requires the xlrd package (see pypi).

    Based on the OKFN Swiss library.
    """
    def __init__(self, resource, sheet=None, encoding=None, skip_rows=None, read_header=True):
        """Creates a XLS spreadsheet data source stream.
        
        :Attributes:
            * resource: file name, URL or file-like object
            * sheet: sheet index number (as int) or sheet name (as str)
            * read_header: flag determining whether first line contains header or not. 
                ``True`` by default.
        """
        self.resource = resource
        self.sheet_reference = sheet
        self.read_header = read_header
        self.header_row = 0
        self.skip_rows = skip_rows
        self._fields = None
        self.close_file = True
        self.encoding = encoding
        self.fields = None

    def initialize(self):
        """Initialize XLS source stream:
        """

        self.file, self.close_file = base.open_resource(self.resource)

        self.workbook = xlrd.open_workbook(file_contents=self.file.read(),
                                           encoding_override=self.encoding)

        if not self.sheet_reference:
            self.sheet_reference = 0

        if type(self.sheet_reference) == int:
            self.sheet = self.workbook.sheet_by_index(self.sheet_reference)
        else:
            self.sheet = self.workbook.sheet_by_name(self.sheet_reference)

        self.row_count = self.sheet.nrows

        self.read_fields()

    def finalize(self):
        if self.file and self.close_file:
            self.file.close()

    def rows(self):
        if not self.sheet:
            raise RuntimeError("XLS Stream is not initialized - there is no sheet")
        if not self.fields:
            raise RuntimeError("Fields are not initialized")
        return XLSRowIterator(self.workbook, self.sheet, self.skip_rows)

    def records(self):
        fields = self.field_names
        for row in self.rows():
            yield dict(zip(fields, row))

    def read_fields(self):
        # FIXME: be more sophisticated and read field types from next row
        if self.read_header:
            row = self.sheet.row_values(self.header_row)
            self.fields = FieldList(row)
            self.skip_rows = self.header_row + 1

class XLSRowIterator(object):
    """
    Iterator that reads XLS spreadsheet
    """
    def __init__(self, workbook, sheet, row_offset=0):
        self.workbook = workbook
        self.sheet = sheet
        self.row_count = sheet.nrows
        if row_offset:
            self.current_row = row_offset
        else:
            self.current_row = 0

    def __iter__(self):
        return self

    def next(self):
        if self.current_row >= self.row_count:
            raise StopIteration

        row = self.sheet.row(self.current_row)
        row = [self._cell_value(cell) for cell in row]
        self.current_row += 1
        return row

    def _cell_value(self, cell):
        """Convert Excel cell into value of a python type
        
        (from Swiss XlsReader.cell_to_python)"""

        # annoying need book argument for datemode
        # info on types: http://www.lexicon.net/sjmachin/xlrd.html#xlrd.Cell-class
        if cell.ctype == xlrd.XL_CELL_NUMBER:
            return float(cell.value)
        elif cell.ctype == xlrd.XL_CELL_DATE:
            # TODO: distinguish date and datetime
            args = xlrd.xldate_as_tuple(cell.value, self.workbook.datemode)
            try:
                return datetime.date(args[0], args[1], args[2])
            except Exception, inst:
                # print 'Error parsing excel date (%s): %s' % (args, inst)
                return None
        elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
            return bool(cell.value)
        else:
            return cell.value

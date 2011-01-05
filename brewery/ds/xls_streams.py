import base

try:
    import xlrd
except ImportError: # xlrd not installed
    pass

class XLSDataSource(base.DataSource):
    """Reading Microsoft Excel XLS Files

    Requires the xlrd package (see pypi).

    Based on the OKFN Swiss library.
    """
    def __init__(self, fileobj, sheet = None, read_header = True):
        """Creates a XLS spreadsheet data source stream.
        
        :Attributes:
            * fileobj: file reference
            * sheet: sheet index number (as int) or sheet name (as str)
            * read_header: flag determining whether first line contains header or not. 
                ``True`` by default.
        """
        self.fileobj = fileobj
        self.sheet_reference = sheet
        self.read_header = read_header
        self.header_row = 0
        self.skip_rows = 0
        self._fields = None
        
    def initialize(self):
        """Initialize XLS source stream:
        """

        if type(self.fileobj) == str:
            self.file = file(self.fileobj, "r")
        else:
            self.file = fileobj

        self.workbook = xlrd.open_workbook(file_contents=self.file.read())

        if not self.sheet_reference:
            self.sheet_reference = 0

        if type(self.sheet_reference) == int:
            self.sheet = self.workbook.sheet_by_index(self.sheet_reference)
        else:
            self.sheet = self.workbook.sheet_by_name(self.sheet_reference)

        self.row_count = self.sheet.nrows
        
        self._read_fields()
        
    def rows(self):
        if not self.sheet:
            raise RuntimeError("XLS Stream is not initialized - there is no sheet")
        return XLSIterator(self.sheet, self.skip_rows)

    @property
    def fields(self):
        if not self._fields:
            raise ValueError("Fields are not initialized in CSV source")
        return self._fields
        
    @fields.setter
    def _set_fields(self, fields):
        self._fields = fields

    def _read_fields(self):
        # FIXME: be more sophisticated and read field types from next row
        if self.read_header:
            row = self.sheet.row_values(self.header_row)
            print row
            self._fields = base.fieldlist(row)
            self.skip_rows = self.header_row + 1

class XLSIterator(object):
    """
    Iterator that reads XLS spreadsheet
    """
    def __init__(self, sheet, row_offset = 0):
        self.sheet = sheet
        self.row_count = sheet.nrows
        self.current_row = row_offset

    def __iter__(self):
        return self

    def next(self):
        if self.current_row >= self.row_count:
            raise StopIteration
            
        row = self.sheet.row_values(self.current_row)
        self.current_row += 1
        return row
        

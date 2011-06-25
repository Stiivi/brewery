#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base
import brewery.metadata as metadata
try:
    import gdata.spreadsheet.text_db
except:
    from brewery.utils import MissingPackage
    gdata = MissingPackage("gdata", "Google data (spreadsheet) source/target")

# Documentation:
# http://gdata-python-client.googlecode.com/svn/trunk/pydocs/

class GoogleSpreadsheetDataSource(base.DataSource):
    """Reading data from a google spreadsheet.
    
    Some code taken from OKFN Swiss library.
    """
    def __init__(self, spreadsheet_key=None, spreadsheet_name=None,
                worksheet_id=None, worksheet_name=None,
                query_string="",
                username=None, password=None):
        """Creates a Google Spreadsheet data source stream.
        
        :Attributes:
            * spreadsheet_key: The unique key for the spreadsheet, this 
                  usually in the the form 'pk23...We' or 'o23...423.12,,,3'.
            * spreadsheet_name: The title of the spreadsheets.
            * worksheet_id: ID of a worksheet
            * worksheet_name: name of a worksheet
            * query_string: optional query string for row selection
            * username: Google account user name
            * password: Google account password
            
        You should provide either spreadsheet_key or spreadsheet_name, if more than one spreadsheet with
        given name are found, then the first in list returned by Google is used.
        
        For worksheet selection you should provide either worksheet_id or worksheet_name. If more than
        one worksheet with given name are found, then the first in list returned by Google is used. If
        no worksheet_id nor worksheet_name are provided, then first worksheet in the workbook is used.
        
        For details on query string syntax see the section on sq under
        http://code.google.com/apis/spreadsheets/reference.html#list_Parameters
        """

        self.spreadsheet_key = spreadsheet_key
        self.spreadsheet_name = spreadsheet_name
        self.worksheet_id = worksheet_id
        self.worksheet_name = worksheet_name
        self.query_string = query_string
        self.username = username
        self.password = password

        self.client = None

        self._fields = None

    def initialize(self):
        """Connect to the Google documents, authenticate.
        """
            
        self.client = gdata.spreadsheet.text_db.DatabaseClient(username=self.username, password=self.password)

        dbs = self.client.GetDatabases(spreadsheet_key=self.spreadsheet_key,
                                        name=self.spreadsheet_name)

        if len(dbs) < 1:
            raise Exception("No spreadsheets with key '%s' or name '%s'" %
                                (self.spreadsheet_key, self.spreadsheet_key))

        db = dbs[0]
        worksheets = db.GetTables(worksheet_id=self.worksheet_id,
                                  name=self.worksheet_name)

        self.worksheet = worksheets[0]
        self.worksheet.LookupFields()

        # FIXME: try to determine field types from next row
        self._fields = metadata.FieldList(self.worksheet.fields)

    def rows(self):
        if not self.worksheet:
            raise RuntimeError("Stream is not initialized (no worksheet)")
        iterator = self.worksheet.FindRecords(self.query_string).__iter__()
        return GDocRowIterator(self.field_names, iterator)

    def records(self):
        if not self.worksheet:
            raise RuntimeError("Stream is not initialized (no worksheet)")
        iterator = self.worksheet.FindRecords(self.query_string).__iter__()
        return GDocRecordIterator(self.field_names, iterator)

class GDocRowIterator(object):
    """
    Iterator that returns immutable list (tuple) of values
    """
    def __init__(self, field_names, iterator):
        self.iterator = iterator
        self.field_names = field_names

    def __iter__(self):
        return self

    def next(self):
        record = self.iterator.next()
        content = record.content
        values = [content[field] for field in self.field_names]
        return list(values)

class GDocRecordIterator(object):
    """
    Iterator that returns records as dict objects
    """
    def __init__(self, field_names, iterator):
        self.iterator = iterator
        self.field_names = field_names

    def __iter__(self):
        return self

    def next(self):
        record = self.iterator.next()
        return record.content

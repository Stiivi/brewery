#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import codecs
import cStringIO
import base
import brewery.metadata

class UTF8Recoder(object):
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8

    From: <http://docs.python.org/lib/csv-examples.html>
    """
    def __init__(self, f, encoding=None):
        if encoding:
            self.reader = codecs.getreader(encoding)(f)
        else: # already unicode so just return f
            self.reader = f

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode('utf-8')

def to_bool(value):
    """Return boolean value. Convert string to True when "true", "yes" or "on"
    """
    return bool(value) or lower(value) in ["true", "yes", "on"]

storage_conversion = {
    "unknown": None,
    "string": None,
    "text": None,
    "integer": int,
    "float": float,
    "boolean": to_bool,
    "date": None
}

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", empty_as_null=False, **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.converters = []
        self.empty_as_null = empty_as_null

    def set_fields(self, fields):
        self.converters = [storage_conversion[f.storage_type] for f in fields]

    def next(self):
        row = self.reader.next()
        result = []

        # FIXME: make this nicer, this is just quick hack
        for i, value in enumerate(row):
            if self.converters:
                f = self.converters[i]
            else:
                f = None

            if f:
                result.append(f(value))
            else:
                uni_str = unicode(value, "utf-8")
                if not uni_str and self.empty_as_null:
                    result.append(None)
                else:
                    result.append(uni_str)
            
        return result

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.

    From: <http://docs.python.org/lib/csv-examples.html>
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        new_row = []
        for value in row:
            if type(value) == unicode or type(value) == str:
                new_row.append(value.encode("utf-8"))
            elif value is not None:
                new_row.append(unicode(value))
            else:
                new_row.append(None)
                
        self.writer.writerow(new_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class CSVDataSource(base.DataSource):
    """docstring for ClassName
    
    Some code taken from OKFN Swiss library.
    """
    def __init__(self, resource, read_header=True, dialect=None, encoding=None,
                 detect_encoding=False, detect_header=False, sample_size=200, 
                 skip_rows=None, empty_as_null=True,fields=None, **reader_args):
        """Creates a CSV data source stream.
        
        :Attributes:
            * resource: file name, URL or a file handle with CVS data
            * read_header: flag determining whether first line contains header
              or not. ``True`` by default.
            * encoding: source character encoding, by default no conversion is
              performed.
            * detect_encoding: read sample from source and determine whether
              source is UTF8 or not
            * detect_headers: try to determine whether data source has headers
              in first row or not
            * sample_size: maximum bytes to be read when detecting encoding
              and headers in file. By default it is set to 200 bytes to
              prevent loading huge CSV files at once.
            * skip_rows: number of rows to be skipped. Default: ``None``
            * empty_as_null: treat empty strings as ``Null`` values
            
        Note: avoid auto-detection when you are reading from remote URL
        stream.
        
        """
        self.read_header = read_header
        self.encoding = encoding
        self.detect_encoding = detect_encoding
        self.detect_header = detect_header
        self.empty_as_null = empty_as_null
        
        self._autodetection = detect_encoding or detect_header
        
        self.sample_size = sample_size
        self.resource = resource
        self.reader_args = reader_args
        self.reader = None
        self.dialect = dialect
        
        self.close_file = False
        self.skip_rows = skip_rows
        self.fields = fields
        
    def initialize(self):
        """Initialize CSV source stream:
        
        #. perform autodetection if required:
            #. detect encoding from a sample data (if requested)
            #. detect whether CSV has headers from a sample data (if
            requested)
        #.  create CSV reader object
        #.  read CSV headers if requested and initialize stream fields
        
        If fields are explicitly set prior to initialization, and header
        reading is requested, then the header row is just skipped and fields
        that were set before are used. Do not set fields if you want to read
        the header.

        All fields are set to `storage_type` = ``string`` and
        `analytical_type` = ``unknown``.
        """

        self.file, self.close_file = base.open_resource(self.resource)

        handle = None
        
        if self._autodetection:
            
            sample = self.file.read(self.sample_size)

            # Encoding test
            if self.detect_encoding and type(sample) == unicode:
                self.encoding = "utf-8"

            if self.detect_header:
                sample = sample.encode('utf-8')
                sniffer = csv.Sniffer()
                self.read_header = sniffer.has_header(sample)

            self.file.seek(0)
            
        if self.dialect:
            if type(self.dialect) == str:
                dialect = csv.get_dialect(self.dialect)
            else:
                dialect = self.dialect
                
            self.reader_args["dialect"] = dialect

        # self.reader = csv.reader(handle, **self.reader_args)
        self.reader = UnicodeReader(self.file, encoding=self.encoding,
                                    empty_as_null=self.empty_as_null,
                                    **self.reader_args)

        if self.skip_rows:
            for i in range(0, self.skip_rows):
                self.reader.next()
                
        # Initialize field list
        if self.read_header:
            field_names = self.reader.next()
            
            # Fields set explicitly take priority over what is read from the
            # header. (Issue #17 might be somehow related)
            if not self.fields:
                fields = [ (name, "string", "default") for name in field_names]
                self.fields = brewery.metadata.FieldList(fields)
            
        self.reader.set_fields(self.fields)
        
    def finalize(self):
        if self.file and self.close_file:
            self.file.close()

    def rows(self):
        if not self.reader:
            raise RuntimeError("Stream is not initialized")
        if not self.fields:
            raise RuntimeError("Fields are not initialized")
        return self.reader

    def records(self):
        fields = self.field_names
        for row in self.reader:
            yield dict(zip(fields, row))

class CSVDataTarget(base.DataTarget):
    def __init__(self, resource, write_headers=True, truncate=True, encoding="utf-8", 
                dialect=None, **kwds):
        """Creates a CSV data target
        
        :Attributes:
            * resource: target object - might be a filename or file-like
              object
            * write_headers: write field names as headers into output file
            * truncate: remove data from file before writing, default: True
            
        """
        self.resource = resource
        self.write_headers = write_headers
        self.truncate = truncate
        self._fields = None
        self.encoding = encoding
        self.dialect = dialect
        self.kwds = kwds

        self.close_file = False
        self.file = None
        
    def initialize(self):
        mode = "w" if self.truncate else "a"

        self.file, self.close_file = base.open_resource(self.resource, mode)

        self.writer = UnicodeWriter(self.file, encoding = self.encoding, 
                                    dialect = self.dialect, **self.kwds)
        
        if self.write_headers:
            self.writer.writerow(self.fields.names())

    def finalize(self):
        if self.file and self.close_file:
            self.file.close()

    def append(self, obj):
        if type(obj) == dict:
            row = []
            for field in self.field_names:
                row.append(obj.get(field))
        else:
            row = obj
                
        self.writer.writerow(row)

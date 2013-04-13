#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import codecs
import cStringIO
from ...objects import *
from ...metadata import *
from ...common import open_resource
from collections import defaultdict
# FIXME: add this
# from ...probes import probe_type
from ...errors import *
from ...stores import DataStore
import os.path

__all__ = (
        "CSVSource",
        "CSVTarget",
        # Depreciated
        "CSVDataSource",
        "CSVDataTarget"
        )

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
    if isinstance(value, basestring):
        value = value.lower()
        return value in ["1", "true", "yes", "on"] and value != "0"
    else:
        return bool(value)

_default_type_converters = {
    "unknown": None,
    "string": None,
    "text": None,
    "integer": int,
    "float": float,
    "boolean": to_bool,
    "date": None,
    "time": None
}

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect="excel", encoding="utf-8", empty_as_null=False,
                 type_converters=None, **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.type_converters = type_converters or _default_type_converters
        self.converters = []
        self.empty_as_null = empty_as_null

    def set_fields(self, fields):
        try:
            self.converters = [self.type_converters[f.storage_type] for f in fields]
        except KeyError as e:
            raise BreweryError("Unknown conversion: %s" % e)

        if not any(self.converters):
            self.converters = None

    def next(self):
        row = self.reader.next()
        result = []

        for i, value in enumerate(row):
            if self.empty_as_null and not value:
                result.append(None)
                continue

            func = self.converters[i] if self.converters else None

            if func:
                result.append(func(value))
            else:
                uni_str = unicode(value, "utf-8")
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

    def __init__(self, f, dialect="excel", encoding="utf-8", **kwds):
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

class CSVStore(DataStore):
    def __init__(self, path, extension=".csv", **kwargs):
        super(CSVStore, self).__init__()
        self.path = path
        self.extension = extension
        self.kwargs = kwargs

    def get_object(self, name):
        """Returns a CSVSource object with filename constructed from store's
        path and extension"""
        if not name.endswith(self.extension):
            name = name + self.extension
        path = os.path.join(self.path, name)
        return CSVSource(path, **self.kwargs)


class CSVSource(DataObject):
    """Comma separated values text file as a data source."""

    _brewery_info = {
        "attributes": [
            {
                "name":"resource",
                "description": "file name, URL or a file handle with CVS data"
            },
            {
                "name":"fields",
                "description": "fields in the file. Should be set if read_header "
                               "or infer_fields is false"
            },
            {
                "name":"fields",
                "description": "flag determining whether first line contains "
                                "header or not. ``True`` by default."
            },
            {
                "name":"encoding",
                "description":"file encoding"
            },
            {
                "name":"read_header",
                "description":"flag whether file header is read or not"
            },
            {
                "name":"infer_fields",
                "description":"Try to determine number and data type of fields "
                              "This option requires the resource to be seek-able. "
                              "Very likely does not work on remote streams."
            },
            {
                "name":"sample_size",
                "description":"Number of rows to read for type detection."
            },
            {
                "name":"skip_rows",
                "description":"number of rows to be skipped"
            },
            {
                "name": "empty_as_null",
                "description": "Treat emtpy strings as NULL values"
            },
            {
                "name": "type_converters",
                "description": "dictionary of data type converters"
            }
        ]
    }

    def __init__(self, resource, read_header=True, dialect=None,
            delimiter=None, encoding=None, sample_size=1024, skip_rows=None,
            empty_as_null=True, fields=None, infer_fields=False,
            type_converters=None):
        """Creates a CSV data source stream.

        :Attributes:
            * `resource`: file name, URL or a file handle with CVS data
            * `read_header`: flag determining whether first line contains header
              or not. ``True`` by default.
            * `encoding`: source character encoding, by default no conversion is
              performed.
            * `fields`: optional `FieldList` object. If not specified then
              `read_header` and/or `infer_fields` should be used.
            * `infer_fields`: try to determine number and data type of fields.
              This option requires the resource to be seek-able, like files.
              Does not work on remote streams.
            * `sample_size`: number of rows to read for type detection if
              `detect_types` is ``True``. 0 means all rows.
              and headers in file. By default it is set to 200 bytes to
              prevent loading huge CSV files at once.
            * `skip_rows`: number of rows to be skipped. Default: ``None``
            * `empty_as_null`: treat empty strings as ``Null`` values
            * `type_converters`: dictionary of converters (functions). It has
              to cover all knowd types.

        Note: avoid auto-detection when you are reading from remote URL
        stream.

        Rules for fields:

        * if `fields` are specified, then they are used, header is ignored
          depending on `read_header` flag
        * if `detect_types` is requested, then types are infered from
          `sample_size` number of rows
        * if `detect_types` is not requested, then each field is of type
          `string` (this is the default)
        """

        """
        RH = request header, FI = fields, IT = infer types

        RH FI IT
         0  0  0 - ERROR
         0  0  1 - detect fields
         1  0  0 - read header, use strings
         1  0  1 - read header, detect types
         0  1  0 - use fields, header as data
         0  1  1 - ERROR
         1  1  0 - ignore header, use fields
         1  1  1 - ERROR
        """
        # FIXME: loosen requirement for type_converters to contain all known
        # types

        self.file = None

        if not any((fields, read_header, infer_fields)):
            raise ArgumentError("At least one of fields, read_header or "
                                "infer_fields should be specified")

        if fields and infer_fields:
            raise ArgumentError("Fields provided and field inference "
                                "requested. They are exclusive, use only one")

        self.read_header = read_header
        self.encoding = encoding
        self.empty_as_null = empty_as_null

        self.resource = resource
        self.close_file = False
        self.reader = None
        self.dialect = dialect
        self.delimiter = delimiter

        self.skip_rows = skip_rows or 0
        self.fields = fields
        self.do_infer_fields = infer_fields
        self.sample_size = sample_size
        self.type_converters = type_converters

        self._initialize()

    def _initialize(self):
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

        # NOTE: this funciton is separate from __init__ for historical reason
        # when there was delayed initialization. I am keeping it separate just
        # in case there will be similar need in the future.

        self.file, self.close_file = open_resource(self.resource)

        args = {}
        if self.dialect:
            if isinstance(self.dialect, basestring):
                args["dialect"] = csv.get_dialect(self.dialect)
            else:
                args["dialect"] = self.dialect
        if self.delimiter:
            args["delimiter"] = self.delimiter

        # self.reader = csv.reader(handle, **self.reader_args)
        self.reader = UnicodeReader(self.file, encoding=self.encoding,
                                    empty_as_null=self.empty_as_null,
                                    type_converters=self.type_converters, **args)

        if self.do_infer_fields:
            self.fields = self.infer_fields()

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
                self.fields = FieldList(fields)

        if not self.fields:
            raise RuntimeError("Fields are not initialized. "
                               "Either read fields from CSV header or "
                               "set them manually")

        self.reader.set_fields(self.fields)

    def infer_fields(self, sample_size=1000):
        """Detects fields from the source. If `read_header` is ``True`` then
        field names are read from the first row of the file. If it is
        ``False`` then field names are `field0`, `field1` ... `fieldN`.

        After detecting field names, field types are detected from sample of
        `sample_size` rows.

        Returns a `FieldList` instance.

        If more than one field type is detected, then the most compatible type
        is returned. However, do not rely on this behavior.

        Note that the source has to be seek-able (like a local file, not as
        remote stream) for detection to work. Stream is reset to its origin
        after calling this method.

        .. note::

            This method is provided for convenience. For production
            environment it is recommended to detect types during development
            and then to use an explicit field list during processing.
        """

        for i in range(0, self.skip_rows):
            self.reader.next()

        if self.read_header:
            field_names = self.reader.next()
        else:
            field_names = None

        types = self.detect_field_types(sample_size)
        self.file.seek(0)

        if field_names and len(types) != len(field_names):
            raise Exception("Number of detected fields differs from number"
                            " of fields specified in the header row")
        if not field_names:
            field_names = ["field%d" % i for i in range(len(types))]

        fields = FieldList()

        for name, types in zip(field_names, types):
            if "integer"in types:
                t = "integer"
            elif "float" in types:
                t = "float"
            elif "date" in types:
                t = "date"
            else:
                t = "string"
            field = Field(name, t)
            fields.append(field)

        return fields

    def detect_field_types(self, sample_size=1000):
        """Read `sample_size` rows from the sourcce and detect field types.
        Works with sources that have `seek()` defined (like file, but not from
        URL source). This method does not rewind the stream - it consumes the
        tested rows."""

        rownum = 0

        probes = defaultdict(set)

        while rownum <= sample_size:
            try:
                row = self.reader.next()
            except StopIteration:
                break

            rownum += 1
            for i, value in enumerate(row):
                probes[i].add(probe_type(value))

        keys = probes.keys()
        keys.sort()

        types = [probes[key] for key in keys]

        return types


    def __del__(self):
        if self.file and self.close_file:
            self.file.close()

    def representations(self):
        return ["rows", "records"]

    def rows(self):
        return self.reader

    def records(self):
        fields = self.fields.names()
        for row in self.reader:
            yield dict(zip(fields, row))

class CSVTarget(DataObject):
    """Comma separated values text file as a data target."""

    _brewery_info = {
        "attributes": [
            {
                "name":"resource",
                "description": "Filename or URL"
            },
            {
                "name": "write_headers",
                "description": "Flag whether first row will contain field names"
            },
            {
                "name": "truncate",
                "description": "If `True` (default) then target file is truncated"
            },
            {
                "name": "encoding",
                "description": "file character encoding"
            },
            {
                "name": "fields",
                "description": "data fields"
            }
        ]
    }

    def __init__(self, resource, write_headers=True, truncate=True,
                 encoding="utf-8", dialect=None,fields=None, **kwds):
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
        self.encoding = encoding
        self.dialect = dialect
        self.fields = fields
        self.kwds = kwds

        self.close_file = False
        self.file = None

        mode = "w" if self.truncate else "a"

        self.file, self.close_file = open_resource(self.resource, mode)

        self.writer = UnicodeWriter(self.file, encoding=self.encoding,
                                    dialect=self.dialect, **self.kwds)

        if self.write_headers:
            if not self.fields:
                raise BreweryError("No fields provided")
            self.writer.writerow(self.fields.names())

        self.field_names = self.fields.names()

    def __del__(self):
        if self.file and self.close_file:
            self.file.close()

    def append(self, row):
        self.writer.writerow(row)

    def append_from(self, obj):
        for row in obj:
            self.append(row)

# FIXME: Backward name compatibility
CSVDataTarget = CSVTarget
CSVDataSource = CSVSource


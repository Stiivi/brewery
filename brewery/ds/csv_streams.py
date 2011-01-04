import csv
import codecs

class UTF8Recoder:
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

class CSVDataSource(ds.DataSource):
    """docstring for ClassName
    
    Some code taken from OKFN Swiss library.
    """
    def __init__(self, fileobj, read_header = True, encoding=None, detect_encoding = False, 
                detect_header = False, sample_size = 200, **kwargs):
        """Creates a CSV data source stream.
        
        :Attributes:
            * cvsobject: file or a file handle with CVS data
            * read_header: flag determining whether first line contains header or not. 
                ``True`` by default.
            * encoding: source character encoding, by default no conversion is performed. 
            * detect_encoding: read sample from source and determine whether source is UTF8 or not
            * detect_headers: try to determine whether data source has headers in first row or not
            * sample_size: maximum bytes to be read when detecting encoding and headers in file. By
                default it is set to 200 bytes to prevent loading huge CSV files at once.
        
        Note: avoid auto-detection when you are reading from remote URL stream.
        
        """
        self.read_header = read_header
        self.encoding = encoding
        self.detect_encoding = detect_encoding
        self.detect_header = detect_header
        
        self._autodetection = detect_encoding or detect_header
        
        self.sample_size = sample_size
        self.fileobj = fileobj
        self.kwargs = kwargs
        self.reader = None
        
    def initialize(self):
        """Initialize CSV source stream:
        
            1.   perform autodetection if required:
            1.1. detect encoding from a sample data (if requested)
            1.2. detect whether CSV has headers from a sample data (if requested)
            2.   create CSV reader object
            3.   read CSV headers if requested and initialize stream fields
        
        """

        if type(self.fileobj) == str:
            self.file = file(self.fileobj, "r")
        else:
            self.file = fileobj

        
        if self._autodetection:
            
            sample = self.file.read(self.sample_size)

            # Encoding test
            if self.detect_encoding:
                if type(sample) == unicode:
                    handle = UTF8Recoder(self.file, None)
                else:
                    sample = sample.decode(self.encoding)
                    handle = UTF8Recoder(self.file, self.encoding)

            if self.detect_header:
                sample = sample.encode('utf-8')
                sniffer = csv.Sniffer()
                self.read_header = sniffer.has_header(sample)

            self.file.seek(0)
            
        
        if not handle:
            handle = UTF8Recoder(self.file, self.encoding)

        self.reader = csv.reader(handle, **self.kwargs)

        # Initialize field list
        if self.read_header:
            fields = self.reader.next()
            self.fields = ds.fieldlist(fields)
        
    def rows(self):
        yield self.reader.next()

class CSVDataTarget(ds.DataTarget):
    def __init__(self, fileobj, write_headers = True, truncate = False):
        self.fileobj = fileobj
        self.write_headers = write_headers
        self.truncate = truncate
    def initialize():
        if type(self.fileobj) == str:
            if self.truncate:
                self.file = file(self.fileobj, "w")
            else:
                self.file = file(self.fileobj, "a")
        else:
            self.file = fileobj
        
        self.writer = csv.writer(self.file)
        
        if self.write_headers:
            self.writer.writerow(self.field_names)
        
    def append(obj):
        
        
        
        
    
Command Line Tools
******************

brewery
=======

Tool for performing brewery framework functionality from command line.

Usage::

    brewery command [command_options]
    
Commands are:

Commands are:

+-----------------------+----------------------------------------------------------------------+
| Command               | Description                                                          |
+=======================+======================================================================+
|``run``                | Run a stream                                                         |
+-----------------------+----------------------------------------------------------------------+
|``graph``              | Generate graphviz structure from stream                              |
+-----------------------+----------------------------------------------------------------------+

``run``

Example::

    brewery run stream.json

``graph``

Generate a graphviz_ graph structure.

.. _graphviz: http://www.graphviz.org/

Example::

    brewery run stream.json > graph.dot
    dot -o graph.png -T png out.dot

mongoaudit
==========

Audit mongo database collections from data quality perspective.

Usage::

    mongoaudit [-h] [-H HOST] [-p PORT] [-t THRESHOLD] [-f {text,json}] database collection

Here is a foo:

========================================= ===============================================================
Argument                                  Description
========================================= ===============================================================
``-h, --help``                            show this help message and exit
``-H HOST, --host HOST``                  host with MongoDB server
``-p PORT, --port PORT``                  port where MongoDB server is listening
``-t THRESHOLD, --threshold THRESHOLD``   threshold for number of distinct values (default is 10)
``-f {text,json}, --format {text,json}``  output format (default is text)
========================================= ===============================================================

The *threshold* is number of distict values to collect, if distinct values is greather than 
threshold, no more values are being collected and *distinct_overflow* will be set. Set to 0 to get
all values. Default is 10.

Measured values
---------------

=================== ============================================================================
Probe               Description
=================== ============================================================================
field               name of a field which statistics are being presented
record_count        total count of records in dataset
value_count         number of records in which the field exist. In RDB table this is equal to 
                    record_count, in document  based databse, such as MongoDB it is number
                    of documents that have a key present (being null or not)
value_ratio         ratio of value count to record count, 1 for relational databases
null_record_ratio   ratio of null value count to record count
null_value_ratio    ratio of null value count to present value count (same as null_record_ration
                    for relational databases)
null_count          number of records where field is null
null_value_ratio    ratio of records
unique_storage_type if there is only one storage type, then this is set to that type
distinct_threshold  
=================== ============================================================================


Example output
--------------

Text output:

::

    flow:
    	storage type: unicode
    	present values: 1257 (10.09%)
    	null: 0 (0.00% of records, 0.00% of values)
    	empty strings: 0
    	distinct values:
    		'spending'
    		'income'
    pdf_link:
    	storage type: unicode
    	present values: 22 (95.65%)
    	null: 0 (0.00% of records, 0.00% of values)
    	empty strings: 0

JSon output:

::

    { ...
        "pdf_link" : {
           "unique_storage_type" : "unicode",
           "value_ratio" : 0.956521739130435,
           "distinct_overflow" : [
              true
           ],
           "key" : "pdf_link",
           "null_value_ratio" : 0,
           "null_record_ratio" : 0,
           "record_count" : 23,
           "storage_types" : [
              "unicode"
           ],
           "distinct_values" : [],
           "empty_string_count" : 0,
           "null_count" : 0,
           "value_count" : 22
        },
        ...
    }
    
.. note::

    This tool will change into generic data source auditing tool and will support all datastores
    that brewery will support, such as relational databases or plain structured files.

################
Object Reference
################

This chapter contains list of object types that can be created using `data_object`:

.. code-block:: python

	source = brewery.data_object("csv_source")
	target = brewery.data_object("sql_table", "data", store=target_store)


csv_source
==========

Comma separated values text file as a data source.


Attributes:

* `resource` – file name, URL or a file handle with CVS data 
* `fields` – fields in the file. Should be set if read_header or infer_fields is false 
* `fields` – flag determining whether first line contains header or not. ``True`` by default. 
* `encoding` – file encoding 
* `read_header` – flag whether file header is read or not 
* `infer_fields` – Try to determine number and data type of fields This option requires the resource to be seek-able. Very likely does not work on remote streams. 
* `sample_size` – Number of rows to read for type detection. 
* `skip_rows` – number of rows to be skipped 
* `empty_as_null` – Treat emtpy strings as NULL values 
* `type_converters` – dictionary of data type converters 





csv_target
==========

Comma separated values text file as a data target.


Attributes:

* `resource` – Filename or URL 
* `write_headers` – Flag whether first row will contain field names 
* `truncate` – If `True` (default) then target file is truncated 
* `encoding` – file character encoding 
* `fields` – data fields 





iterable
========

Wrapped Python iterator that serves as data source. The iterator should
    yield "rows" – list of values according to `fields` 


Attributes:

* `iterable` – Python iterable object 
* `fields` – fields of the iterable 





iterable_records
================

Wrapped Python iterator that serves as data source. The iterator should
    yield "records" – dictionaries with keys as specified in `fields` 


Attributes:

* `iterable` – Python iterable object 
* `fields` – fields of the iterable 





list
====

Wrapped Python list that serves as data source or data target. The list
    content are "rows" – lists of values corresponding to `fields`.

    If list is not provided, one will be created.
    


Attributes:

* `data` – List object. 
* `fields` – fields of the iterable 





mdb_data_source
===============

*No description*


sql_statement
=============

Object representing a SQL statement (from SQLAlchemy).


Attributes:

* `statement` – SQL statement 
* `store` – SQL data store 
* `schema` – default schema 
* `fields` – statement fields (columns) 



Requirements: sqlalchemy



sql_table
=========

Object representing a SQL database table or view (from SQLAlchemy).


Attributes:

* `table` – table name 
* `store` – SQL data store 
* `schema` – default schema 
* `fields` – statement fields (columns) 
* `buffer_size` – size of insert buffer 
* `create` – flag whether table is created *(boolean)*
* `truncate` – flag whether table is truncated *(boolean)*
* `replace` – flag whether table is replaced when created *(boolean)*



Requirements: sqlalchemy



xls_data_source
===============

Reading Microsoft Excel XLS Files

    Requires the xlrd package.
    



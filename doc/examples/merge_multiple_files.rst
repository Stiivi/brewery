Merge multiple CSV (or XLS) Files with common subset of columns into one CSV
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. note::

    This example can be found in the source distribution in
    ``examples/merge_multiple_files`` directory. Referenced CSV files are
    included there as well.

Problem Definition
------------------

We have multiple CSV files, for example with grant listing, from various
sources and from various years. The files have couple common columns, such as
grant receiver, grant amount, however they might contain more additional
information.

Files we have:

* ``grants_2008.csv`` contains `receiver`, `amount`, `date`
* ``grants_2009.csv`` contains `id`, `receiver`, `amount`, `contract_number`,
  `date`
* ``grants_2919.csv`` contains `receiver`, `subject`, `requested_amount`,
  `amount`, `date`


Objective
---------

Create one CSV file by sequentially merging all input CSV files and using all
columns.

Based on our source files we want output csv with fields:

* `receiver`
* `amount`
* `date`
* `id`
* `contract_number`
* `subject`
* `requested_amount`

In addition, we would like to know where the record comes from, therefore we
add `file` which will contain original filename.

Solution
--------

Import brewery and all other necessary packages:

.. code-block:: python

    import brewery
    from brewery import ds
    import sys

Specify sources:

.. code-block:: python

    sources = [
        {"file": "grants_2008.csv", 
         "fields": ["receiver", "amount", "date"]},

        {"file": "grants_2009.csv", 
         "fields": ["id", "receiver", "amount", "contract_number", "date"]},

        {"file": "grants_2010.csv", 
         "fields": ["receiver", "subject", "requested_amount", "amount", "date"]}
    ]


It is highly recommended to explicitly name fileds contained within source
files and do not rely on source headers. In this case we also make sure, that
the field (column) names are normalised. That means that if in one file
receiver is labeled just as "receiver" and in another it is "grant receiver"
we will get the same field.

You can store ``sources`` in an external file, for example as json or yaml and
read it in your script.

Now collect all the fields:


.. code-block:: python

    # Create list of all fields and add filename to store information
    # about origin of data records
    all_fields = brewery.FieldList(["file"])

    # Go through source definitions and collect the fields
    for source in sources:
        for field in source["fields"]:
            if field not in all_fields:
                all_fields.append(field)

Prepare the output stream into ``merged.csv`` and specify fields we have found
in sources and want to write into output:

.. code-block:: python

    out = ds.CSVDataTarget("merged.csv")
    out.fields = brewery.FieldList(all_fields)
    out.initialize()

Go through all sources and merge them:

.. code-block:: python

    for source in sources:
        path = source["file"]

        # Initialize data source: skip reading of headers - we are preparing them ourselves
        # use XLSDataSource for XLS files
        # We ignore the fields in the header, because we have set-up fields
        # previously. We need to skip the header row.
    
        src = ds.CSVDataSource(path,read_header=False,skip_rows=1)
        src.fields = ds.FieldList(source["fields"])
        src.initialize()

        for record in src.records():

            # Add file reference into ouput - to know where the row comes from
            record["file"] = path
            out.append(record)

        # Close the source stream
        src.finalize()

Now you have a sparse CSV files which contains all rows from source CSV files
in one ``merged.csv``.

You can "pretty print" it with::

    $ cat merged.csv | brewery pipe pretty_printer

And you can see the completeness aspect of data quality with simple audit::

    $ cat merged.csv | brewery pipe audit pretty_printer

Variations
----------

You can have a directory with YAML files (one per record/row) as output
instead of one CSV just by changing data stream target. See
:class:`brewery.ds.YamlDirectoryDataTarget` for more information.


.. code-block:: python

    out = ds.YamlDirectoryDataTarget("merged_grants")

Directory ``merged_grants`` must exist before running the script.

Or directly into a SQL database. The following will initialize SQL table
target stream which will remove all existing records from the table before
inserting. Note that the table ``grants`` must exist in database ``opendata``
and must contain columns with names equal to fields specified in
``all_fields``. See :class:`brewery.ds.SQLDataTarget` for more information.

.. code-block:: python

    out = ds.SQLDataTarget(url = "postgres://localhost/opendata",
                           table = "grants",
                           truncate = True)


Refer to source streams and source targets in the API documentation for more
information about possibilities.

.. seealso:: 

    Module :mod:`brewery.ds`
        List of varous data sources and data targets.
    Function :func:`brewery.ds.fieldlist`
        All streams use list of :class:`brewery.ds.Field` objects for field metadata. This function will
        convert list of strings into list of instances of Field class.

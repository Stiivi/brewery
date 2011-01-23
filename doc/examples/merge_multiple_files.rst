Merge multiple CSV (or XLS) Files with common subset of columns into one CSV
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

**Situation**: We have multiple CSV files, for example with grant listing, from various sources and from
various years. The files have couple common columns, such as grant receiver, grant amount, however they
might contain more additional information.

Files we have:

* grants_2008.csv contains `receiver`, `amount`, `date`
* grants_2009.csv contains `id`, `receiver`, `amount`, `contract_number`, `date`
* other_grants_2008.csv contains `receiver`, `subject`, `requested_amount`, `amount`, `date`


**Objective**: Create one CSV file by sequentially merging all input CSV files and using all columns.

Based on our source files we want output csv with fields:

* `receiver`
* `amount`
* `date`
* `id`
* `contract_number`
* `subject`
* `requested_amount`

In addition, we would like to know where the record comes from, therefore we add `file` which will
contain original filename.

Code
----

Import brewery and all other necessary packages:

.. code-block:: python

    import brewery.ds as ds
    import sys

Specify sources:

.. code-block:: python

    sources = [
        {"name": "grants_2008.csv", "fields": ["receiver", "amount", "date"]},
        {"name": "grants_2009.csv", "fields": ["id", "receiver", "amount", "contract_number", "date"]}.
        {"name": "other_grants_2008.csv", "fields": ["receiver", "subject", "requested_amount", "amount", 
                                                    "date"]}
    ]


It is highly recommended to explicitly name fileds contained within source files and do not rely on
source headers. In this case we also make sure, that the field (column) names are normalised. That means
that if in one file receiver is labeled just as "receiver" and in another it is "grant receiver" we will
get the same field.

You can store ``sources`` in an external file, for example as json or yaml and read it in your script.

Now collect all the fields:


.. code-block:: python

    # We want to store filename as origin of each row
    all_fields = ["file"]

    # Go through source definitions and collect the fields
    for source in sources:
        fields = source.get("fields")
        for field in fields:
            if field not in all_fields:
                all_fields.append(field)

Prepare the output stream into ``merged.csv`` and specify fields we have found in sources and want to
write into output:

.. code-block:: python

    out = ds.CSVDataTarget("merged.csv")
    out.fields = ds.fieldlist(all_fields)
    out.initialize()

Go through all sources and merge them:

.. code-block:: python

    for source in sources:
        path = source["path"]
        fields = source.get("fields")

        # Initialize data source: skip reading of headers - we are preparing them ourselves

        src = ds.CSVDataSource(path, read_header = False)
        src.fields = ds.fieldlist(fields)
        src.initialize()

        for record in src.records():

            # Add file reference into ouput - to know where the row comes from
            record["file"] = path
            out.append(record)

        # Close the source stream
        src.finalize()

Now you have a sparse CSV files which contains all rows from source CSV files in one ``merged.csv``.
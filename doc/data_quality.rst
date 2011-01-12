Data Quality
++++++++++++

Functions and classes for measuring data quality.

Example of auditing a CSV file:

.. code-block:: python

    import brewery.ds as ds
    import brewery.dq as dq

    # Open a data stream
    src = ds.CSVDataSource("data.csv")
    src.initialize()

    # Prepare field statistics
    stats = {}
    fields = src.field_names

    for field in fields:
        stats[field] = dq.FieldStatistics(field)

    record_count = 0

    # Probe values
    for row in src.rows():
        for i, value in enumerate(row):
            stats[fields[i]].probe(value)

        record_count += 1

    # Finalize statistics
    for stat in stats.items():
        finalize(record_count)

API
===

.. seealso::

    Module :mod:`brewery.dq`
        API Documentation for data quality

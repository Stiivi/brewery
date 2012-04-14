Data Quality
============

Functions and classes for measuring data quality.

Example of auditing a CSV file:

.. code-block:: python

    from brewery import ds
    from brewery import dq

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

Auditing using :class:`brewery.ds.StreamAuditor`:

.. code-block:: python

    # ... suppose we have initialized source stream as src
    
    # Create autitor stream target and initialize field list
    auditor = ds.StreamAuditor()
    auditor.fields = src.fields
    auditor.initialize()

    # Perform audit for each row from source:
    for row in src.rows():
        auditor.append(row)

    # Finalize results, close files, etc.
    auditor.finalize()

    # Get the field statistics
    stats = auditor.field_statistics


.. autoclass:: brewery.dq.FieldStatistics

.. autoclass:: brewery.dq.FieldTypeProbe


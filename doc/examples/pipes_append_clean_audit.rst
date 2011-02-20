Stream: Append Sources, Clean, Store and Audit
==============================================

**Situation**: We have two directories containing YAML files with donations of same structure (or
at least same subset of fields that we are interested in)::

    donations/source1/
        record_0.yml
        record_1.yml
        record_2.yml
        ...
    donations/source2/
        record_0.yml
        record_1.yml
        record_2.yml
        ...

Some numeric fields are represented as strings, contain leading or trailing spaces, spaces between
numbers.
        
**Objective**: We want to create a CSV file ``donations.csv`` that will contain records from both
directories. Moreover we want to clean the fields: strip spaces from strings and convert mumbers
stored as strings into numbers. Also we want to know, how many of fields are filled in.

Solution
--------

Problem can be solved using following data stream:

.. figure:: pipes_append_clean_audit.png

    Data stream.
    
The stream consists of (from left to right):

* two YAML directory sources
* append node - sequentially concatenate streams
* coalesce types node - fix field values according to specified type, for example convert strings
  into integers for fields of type `integer`
* CSV data target
* audit node
* formatted printer

Code
----

Import brewery pipes:

.. code-block:: python

    import brewery.pipes as pipes


Create a dictionary containing nodes. We will refer to the nodes by name later.


.. code-block:: python

    nodes = {
        "source1": pipes.YamlDirectorySourceNode(path = "donations/source1"),
        "source2": pipes.YamlDirectorySourceNode(path = "donations/source2"),
        "append": pipes.AppendNode(),
        "clean": pipes.CoalesceValueToTypeNode(),
        "output": pipes.CSVTargetNode(resource = "donations.csv"),
        "audit": pipes.AuditNode(distinct_threshold = None),
        "print": pipes.FormattedPrinterNode()
    }

Connect the nodes:

.. code-block:: python

    connections = [ ("source1", "append"),
                    ("source2", "append"),
                    ("append", "clean"),
                    ("clean", "output"),
                    ("clean", "audit"),
                    ("audit", "print")
                    ]

Specify fields that we are going to process from sources. Also specify their types for automated
cleansing. For more information about fields see :class:`brewery.ds.Field` and
:class:`brewery.ds.FieldList`. If you are not creating `FieldList` object directly, then make sure
that you convert an array using :func:`brewery.ds.fieldlist`.

.. code-block:: python

    fields = [  "file",
                ("source_code", "string"),
                ("id", "string"),
                ("receiver_name", "string"),
                ("project", "string"),
                ("requested_amount", "float"),
                ("received_amount", "float"),
                ("source_comment", "string")
            ]

    nodes["source1"].fields = ds.fieldlist(fields)
    nodes["source2"].fields = ds.fieldlist(fields)

Configure nodes:

.. code-block:: python


    nodes["print"].header = u"field                            nulls      empty   distinct\n" \
                             "------------------------------------------------------------"
    nodes["print"].format = u"{field_name:<30.30} {null_record_ratio: >7.2%} "\
                             "{empty_string_count:>10} {distinct_count:>10}"


Crate :class:`brewery.pipes.Steram` and run it:

.. code-block:: python

    stream = pipes.Stream(nodes, connections)
    stream.initialize()
    stream.run()
    stream.finalize()

Stream will create the ``donations.csv`` and will produce a report on standard output that will
look something like this::

    field                            nulls      empty   distinct
    ------------------------------------------------------------
    file                             0.00%          0         32
    source_code                      0.00%          0          2
    id                               9.96%          0        907
    receiver_name                    9.10%          0       1950
    project                          0.05%          0       3628
    requested_amount                22.90%          0        924
    received_amount                  4.98%          0        728
    source_comment                  99.98%          0          2

.. seealso::

    * :ref:`YamlDirectorySourceNode`
    * :ref:`AppendNode`
    * :ref:`CoalesceValueToTypeNode`
    * :ref:`CSVTargetNode`
    * :ref:`AuditNode`
    * :ref:`FormattedPrinterNode`

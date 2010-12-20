Command Line Tools
++++++++++++++++++

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


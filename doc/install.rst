++++++++++++
Installation
++++++++++++

Quick Start
===========

Here are quick installation instructions for the impatient.

Brewery is being developed for Python 2.7, reported to work on 2.6.

Satisfy soft dependencies that cover most of the use cases. For more
information read below.::

    pip install sqlalchemy xlrd

Install brewery::

    pip install brewery

Try:

.. code-block:: python

    import brewery

    URL = "https://raw.github.com/Stiivi/cubes/master/examples/hello_world/data.csv"

    b = brewery.create_builder()
    b.csv_source(URL)
    b.audit(distinct_threshold=None)
    b.pretty_printer()

    b.stream.run()

Or the same from the command line::

    $ curl https://raw.github.com/Stiivi/cubes/master/examples/hello_world/data.csv | \
          brewery pipe audit pretty_printer

Requirements
============

The framework currently does not have any hard dependency on other packages.
All dependencies are optional and you need to install the packages only if
certain features are going to be used.

+-------------------------+---------------------------------------------------------+
|Package                  | Feature                                                 |
+=========================+=========================================================+
| sqlalchemy              | Streams from/to SQL databases. Source:                  |
|                         | http://www.sqlalchemy.org                               |
|                         | Recommended version is > 0.7                            |
+-------------------------+---------------------------------------------------------+
| gdata                   | Google data (spreadsheet) source/target                 |
+-------------------------+---------------------------------------------------------+
| xlrd                    | Reading MS Excel XLS Files. Source:                     |
|                         | http://pypi.python.org/pypi/xlrd                        |
+-------------------------+---------------------------------------------------------+
| PyYAML                  | YAML directory data source/target. Source:              |
|                         | http://pyyaml.org                                       |
+-------------------------+---------------------------------------------------------+
| pymongo                 | MongoDB streams and mongoaudit. Source:                 |
|                         | http://www.mongodb.org/downloads                        |
+-------------------------+---------------------------------------------------------+


Customized Installation
=======================

The project sources are stored in the `Github repository`_.

.. _Github repository: https://github.com/Stiivi/cubes

Download from Github::

    git clone git://github.com/Stiivi/brewery.git
    
Install::

    cd brewery
    python setup.py install

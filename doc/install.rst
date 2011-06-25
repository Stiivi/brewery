Installation
++++++++++++

Requirements
------------

Brewery is being developed for Python 2.7, might work on 2.6.

The framework currently does not have any hard dependency on other packages. All dependencies are
optional and you need to install the packages only if certain features are going to be used.

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

In most cases you can satisfy dependencies just by installing the packages with ``pip install`` or
``easy_install``.


Installation
------------

Install using pip::

    pip install brewery
    
Install using distutils::

    easy_install brewery

Main project source repository is being hosted at Bitbucket: https://bitbucket.org/Stiivi/brewery

Clone mercurial repository from bitbucket::

    hg clone https://bitbucket.org/Stiivi/brewery

Or if you prefer git, then you can clone it from Github: https://github.com/Stiivi/brewery::

    git clone git://github.com/Stiivi/brewery.git

Install from sources after downloading::

    python setup.py install

Version 0.10.0
==============

Quick summary: long planned huge refactoring and change of direction.

Brewery is now general-purpose data processing (ETL) and streaming framework.
Dropping original plans for focus purely on data mining workflows. Goal is to
provide higher level processing functions.

New concept of data objects and their representations is introduced.

Note that Brewery is still experimental framework and API might change. I will
try to announce changes in advance and provide depreciation perion, however it
will be much shorter than after 1.0 release.

News
----

* new concept of data stores and data objects
* data objects can have multiple representations, including stream-like
  iterators
* new module `ops` with low level ETL operations for various backends
  (currently iterators and SQL)
* row transformer supports: copy, set, map, function


Changes
-------

* started huge refactoring of the framework

Fixes
-----

None so far.

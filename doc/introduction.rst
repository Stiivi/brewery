++++++++++++
Introduction
++++++++++++

Brewery is a Python framework for data analysis and data quality measurement.
Principle of the framework are streams of structured data that flow between
processing nodes.

Priorities of the framework are:

* understandability of the analysis process
* auditability of the data being analyzed (frequent use of metadata)
* usability
* versatility

Speed is currently a minor priority of the framework. If you are concerned about
performance, you can still use the framework in your thinking and designing
process, to experience the data you are about to process. Brewery provides
several ways how to get just small samples the data. However, if you know how
to improve any parts of the framework, you are welcome.

Uses
====

When you might consider using brewery?

* data analysis
* data monitoring
* data auditing
* learn more about unknown datasets
* feed auditing and analysis results back to data stores
* streaming data in heterogenous environment - between different stores

Even though Data Brewery is not a full-featured ETL framework it is possible to
use it for simple operations, for playing around with data, piping data from
one store to another.

Modules
=======

The framework consists of several modules:

* :mod:`metadata` – field types and field type operations, describe structure of data (available directly
  from the `brewery` package namespace)
* :mod:`ds` – structured data streams data sources and data targets
* :mod:`streams` – data processing streams
* nodes – analytical and processing stream nodes (see :doc:`/node_reference`)
* :mod:`probes` – analytical and quality data probes

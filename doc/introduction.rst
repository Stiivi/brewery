Introduction
++++++++++++

Brewery is a framework for data analysis and data quality measurement. The framework uses streams of
structured data that flow between processing nodes.

The framework consists of several modules:

* :mod:`metadata` – field types and field type operations, describe structure of data (available directly
  from the `brewery` package namespace)
* :mod:`ds` – structured data streams data sources and data targets
* :mod:`streams` – data processing streams
* nodes – analytical and processing stream nodes (see :doc:`/node_reference`)
* :mod:`probes` – analytical and quality data probes

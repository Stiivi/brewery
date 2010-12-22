OLAP Cubes
**********

Create a model::

    model = brewery.cubes.model_from_path(path)

The *path* is a directory with logical model description files.

.. note::
    Currently only models in a directory are supported, however this will change in the future
    where models will be represented as json files containing all model objects. There are two
    reasons for directory based models: first: easier copying of model objects from model to model
    without any special tools (just filesystem copy), second: original design decision in Ruby
    version of brewery.

Logical Model description
=========================


========================== =============================================
File                       Description
========================== =============================================
model.json                 Core model information
cube_*cube_name*.json      Cube description, one file per cube
dim_*dimension_name*.json  Dimension description, one file per dimension
========================== =============================================


model.json
----------

The ``model.json`` is main model description and looks like this::

    {
    	"name": "public_procurements",
    	"label": "Public Procurements of Slovakia",
    	"description": "Contracts of public procurement winners in Slovakia"
    }


Dimension descriptions in dim_*.json
------------------------------------

The dimension description contains keys:

============== ===================================================
Key            Description
============== ===================================================
name           dimension name
label          human readable name - can be used in an application
levels         dictionary of hierarchy levels
attributes     dictionary of dimension attributes
hierarchies    dictionary of dimension hierarchies
============== ===================================================

Example::

    {
        "name": "date",
        "label": "Dátum",
        "levels": { ... }
        "attributes": { ... }
        "hierarchies": { ... }
    }

Hierarchy levels are described:

================ ================================================================
Key              Description
================ ================================================================
label            human readable name - can be used in an application
key              key field of the level (customer number for customer level,
                 region code for region level, year-month for month level). key
                 will be used as a grouping field for aggregations. Key should be
                 unique within level.
label_attribute  name of attribute containing label to be displayed (customer
                 name for customer level, region name for region level,
                 month name for month level)
attributes       list of other additional attributes that are related to the
                 level. The attributes are not being used for aggregations, they
                 provide additional useful information.
================ ================================================================

Example of month level of date dimension::

    "month": {
        "label": "Mesiac",
        "key": "month",
        "label_attribute": "month_name",
        "attributes": ["month", "month_name", "month_sname"]
    },
    
Example of supplier level of supplier dimension::

    "supplier": {
        "label": "Dodávateľ",
        "key": "ico",
        "label_attribute": "name",
        "attributes": ["ico", "name", "address", "date_start", "date_end",
                        "legal_form", "ownership"]
    }

Hierarchies are described:

================ ================================================================
Key              Description
================ ================================================================
label            human readable name - can be used in an application
levels           ordered list of level names from top to bottom - from least
                 detailed to most detailed (for example: from year to day, from
                 country to city)
================ ================================================================

Example::

    "hierarchies": {
        "default": {
            "levels": ["year", "month"]
        },
        "ymd": {
            "levels": ["year", "month", "day"]
        },
        "yqmd": {
            "levels": ["year", "quarter", "month", "day"]
        }
    }


Cube descriptions in cube_*.json
--------------------------------

============== ====================================================
Key            Description
============== ====================================================
name           dimension name
label          human readable name - can be used in an application
measures       list of cube measures
dimensions     list of cube dimensions
joins          specification of physical table joins
mappings       mapping of logical attributes to physical attributes
============== ====================================================

Example::

    {
        "name": "date",
        "label": "Dátum",
        "dimensions": [ "date", ... ]
        "joins": { ... }
        "mappings": { ... }
    }

Model validation
================
To validate a model do::

    results = model.validate()
    
This will return a list of tuples (result, message) where result might be 'warning' or 'error'.


Classes
=======

.. automodule:: brewery.cubes
    :members:
    :undoc-members:
    
    .. autoclass:: brewery.cubes.Model
        :members:
    .. autoclass:: brewery.cubes.Cube
        :members:
    .. autoclass:: brewery.cubes.Dimension
        :members:
    .. autoclass:: brewery.cubes.Level
        :members:
    .. autoclass:: brewery.cubes.Hierarchy
        :members:
    
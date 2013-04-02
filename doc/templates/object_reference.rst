################
Object Reference
################

This chapter contains list of object types that can be created using `data_object()`:

.. code-block:: python

	source = brewery.data_object("csv_source")
	target = brewery.data_object("sql_table", "data", store=target_store)

{% for object in objects %}
{{object.name|md_heading('=')}}
{% if object.doc %}{{object.doc}}
{% else %}*No description*{%endif%}
{% if object.attributes %}
Attributes:

{% for attr in object.attributes %}* `{{attr.name}}` – {{attr.description}} {%if attr.type%}*({{attr.type}})*{%endif%}
{% endfor %}

{% if object.requirements %}
Requirements: {{ object.requirements | join(',') }}
{% endif %}
{% endif %}
{% endfor %}

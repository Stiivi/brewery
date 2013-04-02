# -*- Encoding: utf8 -*-

"""Generate documentation using metadata and templates"""

import brewery
from brewery.extensions import initialize_namespace
from jinja2 import Environment, FileSystemLoader
import os

TEMPLATES_PATH = os.path.join(os.getcwd(), "templates")
DOCS_PATH = os.getcwd()

def md_heading(value, character='-'):
	heading = "%s\n%s\n" % (value, character*len(value))
	return heading

env = Environment(loader=FileSystemLoader(TEMPLATES_PATH,encoding='utf-8'))
env.filters["md_heading"] = md_heading

template = env.get_template("object_reference.rst")

ns = initialize_namespace("object_types", root_class=brewery.DataObject,
                            suffix=None)

sorted_keys = sorted(ns.keys())

print "objects found: %s" % (sorted_keys, )

objects = []
for key in sorted_keys:
	obj = ns[key]
	info = {}
	info["name"] = key
	print "=== %s" % key
	if obj.__doc__:
		info["doc"] = unicode(obj.__doc__, "utf-8")
	print info
	if hasattr(obj, "_brewery_info"):
		info.update(obj._brewery_info)
	if not info.get("abstract"):
		objects.append(info)

text = template.render(objects=objects)
print objects
target = os.path.join(DOCS_PATH, "object_reference.rst")
with open(target, "w") as f:
	f.write(text.encode("utf-8"))



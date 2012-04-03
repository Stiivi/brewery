from __future__ import print_function
import inspect
import sys
import re
import string
from brewery.nodes import *
import brewery

# sys.path.insert(0, "..")

node_types = [
    {"type": "source", "label": "Sources"},
    {"type": "record", "label": "Record Operations"},
    {"type": "field", "label": "Field Operations"},
    {"type": "target", "label": "Targets"},
]

def decamelize(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)

def underscore(name):
    return re.sub(r' ', r'_', name).lower()

def node_documentation(class_name, node):
    doc = {}
    documentation = inspect.getdoc(node)

    if not documentation:
        documentation = "no documentation"
    elif re.search(r".. +abstract_node", documentation):
        return None
        
    doc["documentation"] = documentation
        
    doc["class_name"] = class_name
    name = decamelize(class_name)
    doc["name"] = name
    doc["identifier"] = node.identifier()

    try:
        info = get_node_info(node)
    except Exception as e:
        info = {}

    node_type = info.get("type")

    if not node_type:
        if issubclass(node, SourceNode):
            node_type = "source"
        elif issubclass(node, TargetNode):
            node_type = "target"
        else:
            node_type = "record"

    doc["type"] = node_type
        
    icon = info.get("icon")
    if not icon:
        icon = underscore(name)

    doc["icon"] = icon
    
    label = info.get("label")
    if not label:
        label = name

    doc["label"] = label
    
    description = info.get("description")
    if description:
        doc["description"] = description
    else:
        doc["description"] = "no description"
    
    doc["output"] = info.get("output")
    doc["attributes"] = info.get("attributes")
    
    return doc

def write_node_doc(doc, f):
        
    doc["underline"] = "-" * len(doc["label"])
    
    f.write(".. _%s:\n\n" % doc["class_name"])
    temp = "${label}\n${underline}\n\n"
    temp += ".. image:: nodes/${icon}.png\n" \
                "   :align: right\n\n" \
                "**Synopsis:** *${description}*\n\n" \
                "**Identifier:** ${identifier} (class: :class:`brewery.nodes.${class_name}`)\n\n" \
                "${documentation}\n\n"
    
    template = string.Template(temp)
    docstring = template.substitute(doc)
    f.write(docstring)
    
    if doc["attributes"]:
        # f.write("\nAttributes\n----------\n")
        f.write("\n.. list-table:: Attributes\n")
        f.write("   :header-rows: 1\n")
        f.write("   :widths: 40 80\n\n")
        f.write("   * - attribute\n")
        f.write("     - description\n")

        for attribute in doc["attributes"]:
            f.write("   * - %s\n" % attribute.get("name"))
            f.write("     - %s\n" % attribute.get("description"))
    
    f.write("\n")
        

def document_nodes_in_module(module):
    nodes_by_type = {}
    
    output = open("node_reference.rst", "w")

    output.write("Node Reference\n"\
                 "++++++++++++++\n\n")

    for name, member in inspect.getmembers(module):
        if inspect.isclass(member) and issubclass(member, Node):
            doc = node_documentation(name, member)
            if doc:
                node_type = doc["type"]
                if node_type in nodes_by_type:
                    nodes_by_type[node_type].append(doc)
                else:
                    nodes_by_type[node_type] = [doc]
                    
    for type_info in node_types:
        label = type_info["label"]
        output.write("%s\n" % label)
        output.write("%s\n\n" % ("=" * len(label)))
        
        node_type = type_info["type"]
        if not node_type in nodes_by_type:
            continue
            
        for node_doc in nodes_by_type[type_info["type"]]:
            write_node_doc(node_doc, output)
    output.close()


document_nodes_in_module(brewery.nodes)
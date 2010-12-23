"""OLAP Cubes"""

import json
import os
import re
from brewery.cubes.cube import *
from brewery.cubes.dimension import *
from brewery.cubes.hierarchy import *
from brewery.cubes.model import *
from brewery.cubes.view_builder import *

def model_from_url(url):
    """Load logical model from a URL.
    
    Argrs:
        url: URL with json representation of the model.
        
    Returs:
        instance of Model
        
    .. warning::
        Not implemented yet
    """
    raise NotImplementedError

def model_from_path(path):
    """Load logical model from a directory specified by path
    
    Argrs:
        path: directory where model is located
        
    Returs:
        instance of Model
    """
    
    if not os.path.isdir(path):
        raise RuntimeError('path should be a directory')
        
    info_path = os.path.join(path, 'model.json')

    if not os.path.exists(info_path):
        raise RuntimeError('main model info %s does not exist' % info_path)

    a_file = open(info_path)
    model_desc = json.load(a_file)
    a_file.close()
    
    if not "name" in model_desc:
        raise KeyError("model has no name")

    # Find model object files and load them
    
    dimensions_to_load = []
    cubes_to_load = []
    
    if not "dimensions" in model_desc:
        model_desc["dimensions"] = {}
    elif type(model_desc["dimensions"]) != dict:
        raise ValueError("dimensions object in model file be a dictionary")
        
    if not "cubes" in model_desc:
        model_desc["cubes"] = {}
    elif type(model_desc["cubes"]) != dict:
        raise ValueError("cubes object in model file should be a dictionary")
    
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if os.path.splitext(filename)[1] != '.json':
                continue
            split = re.split('_', filename)
            prefix = split[0]

            obj_path = os.path.join(dirname, filename)
            if prefix == 'dim' or prefix == 'dimension':
                desc = _model_desc_from_json_file(obj_path)
                if "name" not in desc:
                    raise KeyError("Dimension file '%s' has no name key" % obj_path)
                model_desc["dimensions"][desc["name"]] = desc
            elif prefix == 'cube':
                desc = _model_desc_from_json_file(obj_path)
                if "name" not in desc:
                    raise KeyError("Cube file '%s' has no name key" % obj_path)
                model_desc["cubes"][desc["name"]] = desc

    return model_from_dict(model_desc)
    
def model_from_dict(desc):
    """Create a model from description dictionary
    
    Arguments:
        desc: model dictionary
    """
    
    model = Model(desc["name"], desc)
    return model
        
def _model_desc_from_json_file(object_path):
    """Get a dictionary from reading model json file
    
    Args:
        pbject_path: path within model directory
        
    Returs:
        dict object
    """
    a_file = open(object_path)
    try:
        desc = json.load(a_file)
    except ValueError as e:
        raise SyntaxError("Syntaxt error in %s: %s" % (full_path, e.args))
    finally:
        a_file.close()
        
    return desc
    
def create_cube_view(cube, connection, name):
    """Create denormalized cube view in relational database in a DB2 API compatible connection
    
    Args:
        cube: cube object
        connection: DB2 API connection
        name: view name
    """
    
    builder = ViewBuilder(cube)
    builder.create_view(connection, name)

def create_materialized_cube_view(cube, connection, name):
    """Create denormalized cube view in relational database in a DB2 API compatible connection

    Args:
        cube: cube object
        connection: DB2 API connection
        name: materialized view (table) name
    """

    builder = ViewBuilder(cube)
    builder.create_materialized_view(connection, name)

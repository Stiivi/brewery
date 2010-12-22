"""OLAP Cubes"""

import json
import os
import re
from brewery.cubes.cube import *
from brewery.cubes.dimension import *
from brewery.cubes.hierarchy import *
from brewery.cubes.model import *
from brewery.cubes.view_builder import *

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
    model_info = json.load(a_file)
    a_file.close()
    
    if not "name" in model_info:
        raise KeyError("model has no name")

    model = Model(model_info["name"], model_info)

    # Find model object files and load them
    
    dimensions_to_load = []
    cubes_to_load = []
    
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if os.path.splitext(filename)[1] != '.json':
                continue
            split = re.split('_', filename)
            prefix = split[0]

            full_path = os.path.join(dirname, filename)
            if prefix == 'dim' or prefix == 'dimension':
                dimensions_to_load.append(full_path)
            elif prefix == 'cube':
                cubes_to_load.append(full_path)

    for obj_path in dimensions_to_load:
        info_dict = _model_json_dict(obj_path)
        dim = Dimension(info_dict['name'], info_dict)
        model.add_dimension(dim)

    for obj_path in cubes_to_load:
        info_dict = _model_json_dict(obj_path)
        cube = model.create_cube(info_dict['name'], info_dict)
        model.cubes[info_dict['name']] = cube

    return model
    
def _model_json_dict(object_path):
    """Get a dictionary from reading model json file
    
    Args:
        pbject_path: path within model directory
        
    Returs:
        dict object
    """
    a_file = open(object_path)
    try:
        info_dict = json.load(a_file)
    except ValueError as e:
        raise SyntaxError("Syntaxt error in %s: %s" % (full_path, e.args))
    finally:
        a_file.close()
        
    return info_dict
    
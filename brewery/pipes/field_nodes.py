import base
import re
import copy
import brewery.ds as ds

class FieldMapNode(base.Node):
    """Map fields: rename fields or drop fields.
    
    :Parameters:
        * `map_fields` - dictionary of field name mappings, keys are source fields, values are
          renamed (output) fields
        * `drop_fields` - list of field names to be dropped from stream
    
    """

    def __init__(self, map_fields = None, drop_fields = None):
        super(FieldMapNode, self).__init__()
        if map_fields:
            self.mapped_fields = map_fields
        else:
            self.mapped_fields = {}

        if drop_fields:
            self.dropped_fields = set(drop_fields)
        else:
            self.dropped_fields = set([])
        
    def rename_field(self, source, target):
        """Change field name"""
        self.mapped_fields[source] = target
    
    def drop_field(self, field):
        """Do not pass field from source to target"""
        self.dropped_fields.add(field)

    @property
    def output_fields(self):
        output_fields = ds.FieldList()
        
        for field in self.input.fields:
            if field.name in self.mapped_fields:
                # Create a copy and rename field if it is mapped
                new_field = copy.copy(field)
                new_field.name = self.mapped_fields[field.name]
                output_fields.append(new_field)
            elif field.name not in self.dropped_fields:
                # Pass field if it is not in dropped field list
                output_fields.append(field)
            
        return output_fields

    def run(self):
        self.mapped_field_names = self.mapped_fields.keys()

        # FIXME: change this to row based processing
        for record in self.input.records():
            for field in self.mapped_field_names:
                if field in record:
                    value = record[field]
                    del record[field]
                    record[self.mapped_fields[field]] = value
            for field in self.dropped_fields:
                if field in record:
                    del record[field]
            self.put_record(record)

class TextSubstituteNode(base.Node):
    def __init__(self, field, derived_field = None):
        """Creates a node for text replacement.
        
        :Attributes:
            * `field`: field to be used for substitution (should contain a string)
            * `derived_field`: new field to be created after substitutions. If set to ``None`` then the
              source field will be replaced with new substituted value. Default is ``None`` - same field
              replacement.
        
        """
        super(TextSubstituteNode, self).__init__()

        self.field = field
        self.derived_field = derived_field
        self.substitutions = []
        
    def add_substitution(self, pattern, repl):
        """Add replacement rule for field.
        
        :Parameters:
            * `pattern` - regular expression to be searched
            * `replacement` - string to be used as replacement, default is empty string
        """

        self.substitutions.append( (re.compile(pattern), repl) )
    
    # FIXME: implement this
    # @property
    # def output_fields(self):
    #     pass
        
    def run(self):
        pipe = self.input

        if self.derived_field:
            append = True
        else:
            append = False

        index = self.input_fields.index(self.field)
            
        for row in pipe.rows():
            value = row[index]
            for (pattern, repl) in self.substitutions:
                value = re.sub(pattern, repl, value)
            if append:
                row.append(value)
            else:
                row[index] = value

            self.put(row)

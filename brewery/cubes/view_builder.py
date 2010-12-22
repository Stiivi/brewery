from model import *

def cube_select_statement(cube):
    builder = ViewBuilder(cube)
    builder._create_select_statement()
    return builder.select_statement

class ViewBuilder(object):
    """Create denormalized SQL views based on logical model. The views are used by SQL aggregation browser
    (query generator)"""
    
    def __init__(self, cube):
        """docstring for __init__"""
        self.cube = cube
        self.select_statement = ''
        self.select_expression = None
        pass

    def create_view(self, connection, view_name):
        """Create a denormalized SQL view.
        
        Args:
            connection: db2 API connection
            view_name: name of a view to be created
        """
        
        self._create_select_statement()
        statement = "CREATE OR REPLACE VIEW %s AS %s" % (view_name, self.select_statement)
        connection.execute(statement)

    def create_materialized_view(self, connection, view_name):
        """Create materialized view (a fact table) of denormalized cube structure. Materialized
        views are faster than normal views as there are no joins involved.
        
        Args:
            connection: db2 API connection
            view_name: name of a view (table) to be created
        """

        self._create_select_statement()

        statement = "DROP TABLE IF EXISTS %s" % view_name
        connection.execute(statement)

        statement = "CREATE TABLE %s AS %s" % (view_name, select_statement)
        connection.execute(statement)

        # CREATE INDEX IDX_... on table... (field)

    def _create_select_statement(self):
        self._create_select_expression()
        # self._create_join_expression()

        exprs = []
        exprs.append("SELECT %s" % self.select_expression)
        exprs.append("FROM %s AS %s " % (self.tables, "boo"))
        # exprs.append("FROM %s AS %s " % (fact_table_name, fact_alias))
        # exprs.append(self.join_expression)

        self.select_statement = "\n".join(exprs)

    def _collect_selection(self):
        """Create SELECT expression with list of all cube fields"""
        
        fields = []
        for measure in self.cube.measures:
            fields.append(self.cube.mapped_field(measure) )
        
        # FIXME: use hierarchy
        for dimension in self.cube.dimensions:
            for field in dimension.all_attributes():
                mapping = self.cube.dimension_attribute_mapping(dimension, field) 
                fields.append( (field, mapping) )

        self.selected_fields = fields
        
        self.tables = []
        
        for field in fields:
            split = field[1].split('.')
            table_name = split[0]
            if not table_name in self.tables:
                self.tables.append(table_name)
            
    def _create_select_expression(self):
        self._collect_selection()
        # self.select_expression = ', '.join([field[1] in  self.selected_fields)

    def physical_field(self, logical_field):
        """Return physical field reference from logical field"""
        return self.cube.mapped_field(logical_field)

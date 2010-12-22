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
        self._create_join_expression()

        exprs = []
        exprs.append("SELECT %s" % self.select_expression)
        # exprs.append("FROM %s AS %s " % (self.tables, "boo"))
        exprs.append("FROM %s AS %s" % (self.fact_table, self.fact_alias))
        exprs.append(self.join_expression)

        self.select_statement = "\n".join(exprs)

    def _collect_selection(self):
        """Create SELECT expression with list of all cube fields"""
        
        fields = []
        for measure in self.cube.measures:
            fields.append( self.cube.fact_field_mapping(measure) )
        
        # FIXME: use hierarchy
        for dimension in self.cube.dimensions:
            for field in dimension.all_attributes():
                mapping = self.cube.dimension_field_mapping(dimension, field) 
                fields.append( mapping )

        self.selected_fields = fields
        
        aliases = ["%s AS %s" % (field[0], self.quote_field(field[1])) for field in fields]
        self.select_expression = ', '.join(aliases)

            
    def _create_select_expression(self):
        self._collect_selection()
        if not self.cube.fact:
            raise ValueError("Factless cubes not supported, please specify fact name in cube '%s'" % self.cube.name)
        self.fact_table = self.cube.fact
        self.fact_alias = self.cube.fact
        # self.select_expression = ', '.join([field[1] in  self.selected_fields)

    def _create_join_expression(self):
        expressions = []

        joins = self.cube.joins

        for join in joins:
            master_split = join["master"].split('.')
            detail_split = join["detail"].split('.')
            master_table = master_split[0]
            detail_table = detail_split[0]
            master_key = master_split[1]
            detail_key = master_split[1]
            if "alias" in join:
                alias = join["alias"]
            else:
                alias = detail_table

            expr = "JOIN %s AS %s ON (%s.%s = %s.%s)" \
                % (detail_table, alias, alias, detail_key, master_table, master_key)
            expressions.append(expr)

        self.join_expression = "\n".join(expressions)

    def physical_field(self, logical_field):
        """Return physical field reference from logical field"""
        return self.cube.mapped_field(logical_field)

    def quote_field(self, field):
        """Quote field name"""
        return '"%s"' % field
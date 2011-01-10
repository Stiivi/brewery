import aggregation_browser

try:
    import pymongo
    import bson
except ImportError:
    pass

class MongoAggregationBrowser(aggregation_browser.AggregationBrowser):
    """MongoDB Aggregation Browser"""
        
    def __init__(self, cube, collection):
        """Create mongoDB Aggregation Browser
        """
        super(MongoAggregationBrowser, self).__init__(cube)
        self.collection = collection
    
    def aggregate(self, cuboid, measure):

        values = {
            "collection": "entity",
            "dimension_list": "time:this.time",
            "measure_agg": "amount_sum",
            "measure": "amount"
        }

        map_function = '''
        function() {
            emit(
                {%(dimension_list)s},
                {%(measure_agg)s: this.%(measure)s, record_count:1}
            );
        }''' % values

        reduce_function = '''
        function(key, vals) {
            var ret = {%(measure_agg)s:0, record_count:1};
            for(var i = 0; i < vals.length; i++) {
                ret.%(measure_agg)s += vals[i].%(measure_agg)s;
                ret.record_count += vals[i].record_count;
            }
            return ret;
        }\n''' % values

        print("--- MAP: %s" % map_function)
        print("--- REDUCE: %s" % reduce_function)

        map_reduce = '''
        db.runCommand({
        mapreduce: "entry",
        map: %(map)s,
        reduce: %(reduce)s,
        out: { inline : 1}
        })
        '''
        code = bson.code.Code(map_reduce)


        output = "{ inline : 1}"
        map_code = bson.code.Code(map_function)
        reduce_code = bson.code.Code(reduce_function)

        # result = self.collection.map_reduce(map_code, reduce_code, out = 'cube')
        result = self.collection.database.command("mapreduce", "entry",
                                            map = map_function,
                                            reduce = reduce_function,
                                            out = { "inline" : 1 })
        return result

class MongoCubeGenerator(object):
    def __init__(self, cube):
        pass
    
    
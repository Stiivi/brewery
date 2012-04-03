"""
Data Brewery Example

Aggregate a remote CSV file.
"""
import brewery

main = brewery.create_builder()

main.csv_source("https://raw.github.com/Stiivi/cubes/master/examples/hello_world/data.csv")
main.node.fields = brewery.FieldList([
                                "category_code",
                                "category",
                                "subcategory_code",
                                "subcategory", 
                                "line_item", 
                                "year", 
                                ["amount", "float"] 
                            ])
main.aggregate(keys=["year", "category"], measures=["amount"])
main.field_map(keep_fields=["year", "category", "amount_sum"])
main.pretty_printer()

main.stream.run()

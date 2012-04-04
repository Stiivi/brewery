"""
Data Brewery - http://databrewery.org

Example: How to use a generator function as a streaming data source.

"""


import brewery
import random

# Create a generator function
def generator(count=10, low=0, high=100):
    for i in range(0, count):
        yield [i, random.randint(low, high)]
        
# Create stream builder (HOM-based)
main = brewery.create_builder()

main.generator_function_source(generator, fields=brewery.FieldList(["i", "roll"]))

# Configure node with this:
#
# main.node.kwargs = {"count":100, "high":10}

# Uncomment this:
#
# fork = main.fork()
# fork.csv_target("random.csv")

main.formatted_printer()
main.stream.run()

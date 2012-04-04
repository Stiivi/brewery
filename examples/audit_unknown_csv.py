"""
Brewery Example - basic audit of "unknown" CSV file.

Shows:

* record count
* null count and ratio
* number of distinct values

"""

import brewery

# Create stream builder
main = brewery.create_builder()

URL = "http://databank.worldbank.org/databank/download/WDR2011%20Dataset.csv"

main.csv_source(URL,encoding="latin-1") # <-- source node
main.audit(distinct_threshold=None)

# Uncomment following later:
# main.value_threshold( [["null_record_ratio", 0.4]] )
# main.set_select( "null_record_ratio_bin", ["high"])

main.pretty_printer() # <-- target node

# Run the stream
main.stream.run()

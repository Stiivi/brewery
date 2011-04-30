#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brewery.ds as ds
from brewery.nodes import Node
import urllib

SWIKI_BASEURL = "http://api.scraperwiki.com/api/1.0/datastore/getdata"

class ScraperWikiDataSource(ds.CSVDataSource):
    def __init__(self, name):
        """Creates a data source that will read data from scraperwiki scraper"""
        self.scraper_name = name
        self.stream = None
        
        params = {
            "name": self.scraper_name,
            "format": "csv"
        }

        params_str = urllib.urlencode(params)
        data_url = SWIKI_BASEURL + "?" + params_str

        super(ScraperWikiDataSource, self).__init__(data_url, read_header = True, 
                                                    encoding = "utf-8")

class ScraperWikiSourceNode(Node):
    """Source node that reads data from a Scraper Wiki scraper.
    
    See: http://scraperwiki.com/
    """
    __node_info__ = {
        "label" : "Scraper Wiki Source",
        "icon": "generic_node",
        "description" : "Read data from a Scraper Wiki scraper.",
        "attributes" : [
            {
                 "name": "scraper",
                 "description": "Scraper name"
            }
        ]
    }

    def __init__(self, scraper = None):
        super(ScraperWikiSourceNode, self).__init__()
        self.scraper = scraper

    def initialize(self):
        self.stream = ScraperWikiDataSource(self.scraper)
        self.stream.initialize()

    @property
    def output_fields(self):
        return self.stream.fields
        
    def run(self):
        for row in self.stream.rows():
            self.put(row)
    
    def finalize(self):
        self.stream.finalize()
        
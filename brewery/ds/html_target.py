#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base

class SimpleHTMLDataTarget(base.DataTarget):
    def __init__(self, resource, html_header = True, html_footer = None, 
                 write_headers = True, table_attributes = None,
                ):
        """Creates a HTML data target with simple naive HTML generation. No package that generates
        document node tree is used, just plain string concatenation.
        
        :Attributes:
            * resource: target object - might be a filename or file-like object - you can stream
              HTML table data into existing opened file.
            * write_headers: create table headers, default: True. Field labels will be used,
              if field has no label, then fieln name will be used.
            * table_attributes: <table> node attributes such as ``class``, ``id``, ...
            * html_header: string to be used as HTML header. If set to ``None`` only <table> will
              be generated. If set to ``True`` then default header is used. Default is ``True``.
            * html_header: string to be used as HTML footer. Works in similar way as to html_header.

        Note: No HTML escaping is done. HTML tags in data might break the output.
        """

        self.resource = resource
        self.write_headers = write_headers
        self.table_attributes = table_attributes
        
        if html_header == True:
            self.html_header = """
            <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
            <html>
            <head>
            <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
            </head>
            <body>
            """
        elif html_header and html_header != True:
            self.html_header = html_header
        else:
            self.html_header = ""
            
        if html_footer == True:
            self.html_footer = """
            </body>"""
        elif html_footer and html_footer != True:
            self.html_footer = html_footer
        else:
            self.html_footer = ""
        
    def initialize(self):
        self.handle, self.close_file = base.open_resource(self.resource, "w")

        if self.html_header:
            self.handle.write(self.html_header)

        attr_string = u""

        if self.table_attributes:
            for attr_value in self.table_attributes.items():
                attr_string += u' %s="%s"\n' % attr_value

        string = u"<table%s>\n" % attr_string
            
        if self.write_headers:
            string += u"<tr>"
            for field in self.fields:
                if field.label:
                    header = field.label
                else:
                    header = field.name
                
                string += u"  <th>%s</th>\n" % header

            string += u"</tr>\n"
            
        self.handle.write(string)
        
    def append(self, obj):
        if type(obj) == dict:
            row = []
            for field in self.field_names:
                row.append(obj.get(field))
        else:
            row = obj

        string = u"<tr>"
        for value in row:
            string += u"  <td>%s</td>\n" % value

        string += u"</tr>\n"
        self.handle.write(string.encode('utf-8'))

    def finalize(self):
        string = u"</table>"
        self.handle.write(string)

        if self.html_footer:
            self.handle.write(self.html_footer)
        
        if self.close_file:
            self.handle.close()
        
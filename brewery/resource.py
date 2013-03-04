# -*- coding: utf-8 -*-
""" URI Resource and file utilities - requires Requests framework"""

# TODO: file utilities should be in a separate extension package

import urlparse
import os

__all__ = (
            "newest_file"
        )

try:
    import requests
except ImportError:
    from brewery.common import MissingPackage
    requests = MissingPackage("requests", "Resources",
                                "http://docs.python-requests.org/en/latest/")

#

def describe_resource(url):
    pass

def object_from_url(url):
    """Creates an object from URL"""
    requests.get
    result = urlparse.urlparse(url)
    raise NotImplemented


def newest_file(path, extension=None):
    """Returns the newest file in directory `path`. If `ext` is not ``None``
    then the file should have extension `ext`"""

    newest_time = None
    newest_file = None
    for f in os.listdir(path):
        base, ext = os.path.splitext(f)
        # If extension is required, then we look only for files with
        # that extension
        if extension and ext != "."+extension:
            continue

        path = os.path.join(path, f)
        time = os.stat(path).st_ctime
        if time > newest_time:
            newest_file = path
            newest_time = time

    return newest_file


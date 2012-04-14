#!/usr/bin/env python
# -*- coding: utf-8 -*-

class ProbeSet(object):
    """Set of probes"""
    def __init__(self, probes=None):
        """Creates a probe-set which acts as multi-probe. `probes` should be
        a list of probes."""
        super(ProbeSet, self).__init__()
        self.probes = probes

    def probe(self, value):
        """Probe the value in all of the probes."""
        for probe in self.probes:
            probe.probe(value)

    def finalize(self):
        """Finalize all probes."""
        for probe in self.probes:
            probe.finalize()

class FieldTypeProbe(object):
    """Probe for guessing field data type

    Attributes:
        * field: name of a field which statistics are being presented
        * storage_types: found storage types
        * unique_storage_type: if there is only one storage type, then this is set to that type
    """
    def __init__(self, field):
        self.field = field

        self.storage_types = set()

        self.null_count = 0
        self.empty_string_count = 0

    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

        if value is None:
            self.null_count += 1
        if value == '':
            self.empty_string_count += 1

    @property
    def unique_storage_type(self):
        """Return storage type if there is only one. This should always return a type in relational
        databases, but does not have to in databases such as MongoDB."""

        if len(self.storage_types) == 1:
            return list(self.storage_types)[0]
        return None

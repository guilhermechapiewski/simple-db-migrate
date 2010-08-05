#!/usr/bin/env python
# encoding: utf-8
"""
Module that contains the migrations model after they've been parsed from disk.
"""

from os.path import split

class Migration(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = split(filepath)[-1]
        self.version = self.filename.split('_')[0]
        self.title = '_'.join(self.filename.split('_')[1:])\
                        .replace('.migration', '')

    
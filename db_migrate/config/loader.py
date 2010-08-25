#!/usr/bin/env python
# encoding: utf-8

'''Dynamically loads python modules, even 
if their filenames have different extensions'''

from os.path import exists
import md5
import imp

DO_OPEN = open

def load_module(code_path):
    '''Loads the specified file as a python module
    and returns it.'''

    if not exists(code_path):
        raise ValueError("File not found at %s. Can't load module." % code_path)

    try:
        fin = DO_OPEN(code_path, 'rb')
        md5_hash = md5.new(code_path).hexdigest()

        return imp.load_source(md5_hash, code_path, fin)
    finally:
        fin.close()
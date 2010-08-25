#!/usr/bin/env python
# encoding: utf-8

from os.path import exists, dirname, basename
import codecs
import sys
import md5
import imp
import traceback

do_open = open

def load_module(code_path):
    if not exists(code_path):
        raise ValueError("File not found at %s. Can't load module." % code_path)

    try:
        fin = do_open(code_path, 'rb')
        md5_hash = md5.new(code_path).hexdigest()

        return imp.load_source(md5_hash, code_path, fin)
    finally:
        fin.close()
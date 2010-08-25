#!/usr/bin/env python
# encoding: utf-8

from os.path import dirname, abspath, join

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations

import db_migrate.config.loader as loader

def test_loader_raises_when_file_not_found():
    try:
        loader.load_module('/tmp/fake_module.py')
    except ValueError, err:
        assert str(err) == "File not found at /tmp/fake_module.py. Can't load module."
        return

    assert False, 'Should not have gotten this far.'

def test_loader_returns_module():
    path = abspath(join(dirname(__file__), 'data', 'helloworld.py'))
    module = loader.load_module(path)

    assert module
    assert hasattr(module, 'hello_world')
    assert module.hello_world() == "Hello World"
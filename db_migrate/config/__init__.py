#!/usr/bin/env python
# encoding: utf-8

'''Configuration classes for simple-db-migrate'''

from db_migrate.config.loader import load_module

class Config(object):
    '''Loads configurations from a file or statically.'''
    def __init__(self, **kwargs):
        '''Initializer for Config. Pass in arguments 
        as keyword arguments to load it.'''
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    @classmethod
    def static(cls, **kwargs):
        '''Creates a new config out of the specified options'''
        return Config(**kwargs)

    @classmethod
    def load_file(cls, path):
        '''Loads a simple-db-migrate config file.'''
        conf_module = load_module(path)

        def get_attr(key):
            '''helper function to get the key or none'''
            return hasattr(conf_module, key) and getattr(conf_module, key) \
                                             or None

        instance = Config(host=get_attr('DATABASE_HOST'),
                          user=get_attr('DATABASE_USER'),
                          password=get_attr('DATABASE_PASSWORD'),
                          db=get_attr('DATABASE_NAME'),
                          port=get_attr('DATABASE_PORT'),
                          migrations_dir=get_attr('DATABASE_MIGRATIONS_DIR'))

        return instance
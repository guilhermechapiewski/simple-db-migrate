#!/usr/bin/env python
# encoding: utf-8

from db_migrate.config.loader import load_module

class Config(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    @classmethod
    def load_file(cls, path):
        conf_module = load_module(path)

        def get_attr(key):
            return hasattr(conf_module, key) and getattr(conf_module, key) \
                                             or None

        instance= Config(host=get_attr('DATABASE_HOST'),
                         user=get_attr('DATABASE_USER'),
                         password=get_attr('DATABASE_PASSWORD'),
                         db=get_attr('DATABASE_NAME'),
                         port=get_attr('DATABASE_PORT'),
                         migrations_dir=get_attr('DATABASE_MIGRATIONS_DIR'))

        return instance
#!/usr/bin/env python
# encoding: utf-8

from db_migrate import lib
from sqlalchemy.engine import create_engine

class Db(object):
    def __init__(self, config):
        self.config = config
        self.connection = None

        self.connection_strings = {
            'postgre' : 'postgresql://%(user)s:%(pass)s@%(host)s/%(db)s', 
            'mysql': 'mysql://%(user)s:%(pass)s@%(host)s/%(db)s',
            'oracle': 'oracle://%(user)s:%(pass)s@%(host)s:%(port)d/%(db)s',
            'mssql': 'mssql://%(db)s',
            'sqlite': 'sqlite:///%(host)s',
            'in-memory-sqlite': 'sqlite://'
        }

    def __del__(self):
        self.close()

    def connect(self):
        engine = create_engine(self.connection_string)
        self.connection = engine.connect()
    
    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None

    @property
    def connection_string(self):
        return self.connection_strings[self.config.dbtype] % {
            'user': self.config.user,
            'pass': self.config.password,
            'host': self.config.host,
            'port': self.config.port,
            'db' : self.config.db
        }
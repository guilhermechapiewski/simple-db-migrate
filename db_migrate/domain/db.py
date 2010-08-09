#!/usr/bin/env python
# encoding: utf-8

'''
Module responsible for managing Database interaction.
Must be db-agnostic as far as the supported Dbs go.
'''

from db_migrate import lib #sets the lib folder to be the first in PYTHONPATH
from sqlalchemy.engine import create_engine

from core.exceptions import MigrationException

class Db(object):
    '''Class responsible for managing the communication with the database.'''

    def __init__(self, config):
        '''Initializes a Db object with the given config.'''

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
        '''
        Destroys an instance of a Db object making sure 
        to close the connection.
        '''
        self.close()

    def connect(self):
        '''
        Connects to the database and sets the connection attribute 
        to the active connection.
        '''
        self.engine = create_engine(self.connection_string)
        self.connection = self.engine.connect()

    def close(self):
        '''
        Closes the connection if one is active. Does nothing otherwise.
        '''
        if self.connection:
            self.connection.close()
        self.connection = None

    def execute(self, sql):
        if not self.connection:
            self.connect()
        return self.connection.execute(sql)

    def query_scalar(self, sql):
        result = self.execute(sql)

        return result[0][0]

    @property
    def connection_string(self):
        '''Returns the proper connection string according to config.'''
        return self.connection_strings[self.config.dbtype] % {
            'user': self.config.user,
            'pass': self.config.password,
            'host': self.config.host,
            'port': self.config.port,
            'db' : self.config.db
        }

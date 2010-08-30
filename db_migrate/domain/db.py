#!/usr/bin/env python
# encoding: utf-8

'''
Module responsible for managing Database interaction.
Must be db-agnostic as far as the supported Dbs go.
'''

from sqlalchemy.schema import MetaData
from sqlalchemy.engine import create_engine

from db_migrate import lib #sets the lib folder to be the first in PYTHONPATH

class Db(object):
    '''Class responsible for managing the communication with the database.'''

    def __init__(self, config):
        '''Initializes a Db object with the given config.'''

        self.config = config
        self.connection = None
        self.engine = None
        self.meta = MetaData()

        self.connection_strings = {
            'postgre' : 'postgresql://%(user)s:%(pass)s@%(host)s/%(db)s', 
            'mysql': 'mysql://%(user)s:%(pass)s@%(host)s/%(db)s',
            'oracle': 'oracle://%(user)s:%(pass)s@%(host)s:%(port)d/%(db)s',
            'mssql': 'mssql://%(db)s',
            'sqlite': 'sqlite:///%(host)s',
            'in-memory-sqlite': 'sqlite://'
        }
        
        self.main_database_names = {
            'postgre': '', 
            'mysql': 'mysql',
            'oracle': '',
            'mssql': '',
            'sqlite': '',
            'in-memory-sqlite': ''
        }

    def __del__(self):
        '''
        Destroys an instance of a Db object making sure 
        to close the connection.
        '''
        self.close()

    def connect(self, to_main_database=False):
        '''
        Connects to the database and sets the connection attribute 
        to the active connection.
        '''
        conn_str = self.connection_string
        if to_main_database:
            conn_str = self.main_database_connection_string

        self.engine = create_engine(conn_str)
        self.connection = self.engine.connect()
        self.meta.bind = self.engine

    def close(self):
        '''
        Closes the connection if one is active. Does nothing otherwise.
        '''
        if self.connection:
            self.connection.close()
        self.connection = None

    def execute(self, sql, to_main_database=False):
        '''Executes the sql and returns the cursor.'''
        if not self.connection:
            self.connect(to_main_database)
        return self.connection.execute(sql)

    def query_scalar(self, sql, to_main_database=False):
        '''Executes the sql and returns the first item from the first row.'''
        result = self.execute(sql, to_main_database)

        return result.scalar()

    def create_database(self):
        '''Creates a database with the config specified name.'''
        sql = "CREATE DATABASE IF NOT EXISTS %s" % self.config.db

        self.execute(sql, to_main_database=True)

    def drop_database(self):
        '''Drops the database with the config specified name.'''
        sql = "DROP DATABASE IF EXISTS %s" % self.config.db

        self.execute(sql, to_main_database=True)

    def create_table(self, table):
        '''Creates a table with the given fields.'''
        if not self.connection:
            self.connect()

        table.create(checkfirst=True)

    def drop_table(self, table):
        '''Creates a table with the given fields.'''
        if not self.connection:
            self.connect()

        table.drop(checkfirst=True)

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

    @property
    def main_database_connection_string(self):
        '''Returns the proper connection string according to config.'''
        return self.connection_strings[self.config.dbtype] % {
            'user': self.config.user,
            'pass': self.config.password,
            'host': self.config.host,
            'port': self.config.port,
            'db' : self.main_database_names[self.config.dbtype]
        }

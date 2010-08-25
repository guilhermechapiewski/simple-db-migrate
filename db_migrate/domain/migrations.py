#!/usr/bin/env python
# encoding: utf-8
"""
Module that contains the migrations model after they've been parsed from disk.
"""

import re
from os.path import split, exists

from db_migrate.domain.errors import InvalidMigrationFilenameError, \
                                     MigrationFileDoesNotExistError, \
                                     InvalidMigrationFileError
from db_migrate.config.loader import load_module

class Migration(object):
    '''
    File system migration.
    '''
    MIGRATION_FILES_EXTENSION = ".migration"
    MIGRATION_FILES_MASK = r"[0-9]{14}\w+%s$" % MIGRATION_FILES_EXTENSION

    def __init__(self, filepath):
        '''
        Initializes the migration.
        Parses the filename for version and title.
        '''
        self.filepath = filepath
        self.filename = split(filepath)[-1]

        if not self.is_valid_filename(self.filename):
            error = ("The file '%s' is not a migration and " + \
                     "cannot be parsed. Migrations should have " + \
                     "the .migration extension.") % self.filepath
            raise InvalidMigrationFilenameError(error)

        self.version = self.filename.split('_')[0]
        self.title = '_'.join(self.filename.split('_')[1:])\
                        .replace('.migration', '')
        self.up_statements = []
        self.down_statements = []

    @staticmethod
    def is_valid_filename(filename):
        '''
        Verifies if a given migration name is valid.
        Being valid means matching the migration regular expression.
        '''
        match = re.match(Migration.MIGRATION_FILES_MASK, 
                         filename, 
                         re.IGNORECASE)
        return match != None

    def load(self):
        '''
        Loads this migration from the disk. 
        Gets both the up and down statements.
        '''
        if not exists(self.filepath):
            raise MigrationFileDoesNotExistError(('The migration at %s does'+
                                            ' not exist') % self.filepath)

        migration_module = load_module(self.filepath)

        if not hasattr(migration_module, 'SQL_UP') or \
           not hasattr(migration_module, 'SQL_DOWN'):
            msg = ("Migration file at '%s' it not well-formed. " + \
                   "It should have both SQL_UP and SQL_DOWN variable " + \
                   "assignments.") % self.filepath
            raise InvalidMigrationFileError(msg)

        self.up_statements = \
                    Migration.parse_sql_statements(migration_module.SQL_UP)
        self.down_statements = \
                    Migration.parse_sql_statements(migration_module.SQL_DOWN)

    @classmethod
    def parse_sql_statements(cls, sql):
        '''
        Given a string with sql statements split by ';' returns
        a tuple with all the statements.
        '''
        all_statements = []
        last_statement = ''
        
        for statement in sql.split(';'):
            if len(last_statement) > 0:
                curr_statement = '%s;%s' % (last_statement, statement)
            else:
                curr_statement = statement
            
            single_quotes = curr_statement.count("'")
            double_quotes = curr_statement.count('"')
            left_parenthesis = curr_statement.count('(')
            right_parenthesis = curr_statement.count(')')
            
            if single_quotes % 2 == 0 and \
               double_quotes % 2 == 0 and \
               left_parenthesis == right_parenthesis:
                all_statements.append(curr_statement)
                last_statement = ''
            else:
                last_statement = curr_statement
            
        return [s.strip() for s in all_statements if s.strip() != ""]
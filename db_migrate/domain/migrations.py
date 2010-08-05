#!/usr/bin/env python
# encoding: utf-8
"""
Module that contains the migrations model after they've been parsed from disk.
"""

import codecs
import re
from os.path import split, exists

from db_migrate.domain.errors import InvalidMigrationFilenameError, \
                                     MigrationFileDoesNotExistError, \
                                     InvalidMigrationFileError

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
        SQL_UP = None
        SQL_DOWN = None

        if not exists(self.filepath):
            raise MigrationFileDoesNotExistError(('The migration at %s does'+
                                            ' not exist') % self.filepath)
        
        #TODO - Really don't like this. exec is evil.
        migration_file = codecs.open(self.filepath, "rU", "utf-8")
        exec(migration_file.read())
        migration_file.close()

        if not SQL_UP or not SQL_DOWN:
            msg = ("Migration file at '%s' it not well-formed. " + \
                   "It should have both SQL_UP and SQL_DOWN variable " + \
                   "assignments.") % self.filepath
            raise InvalidMigrationFileError(msg)

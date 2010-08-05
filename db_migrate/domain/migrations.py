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

        if not self.is_file_name_valid(self.filename):
            error = ("The file '%s' is not a migration and " + "cannot be parsed. Migrations should have the .migration extension.") % self.filepath
            raise InvalidMigrationFilenameError(error)

        self.version = self.filename.split('_')[0]
        self.title = '_'.join(self.filename.split('_')[1:])\
                        .replace('.migration', '')

    @staticmethod
    def is_file_name_valid(file_name):
        match = re.match(Migration.MIGRATION_FILES_MASK, file_name, re.IGNORECASE)
        return match != None

    def load(self):
        '''
        Loads this migration from the disk. 
        Gets both the up and down statements.
        '''
        if not exists(self.filepath):
            raise MigrationFileDoesNotExistError(('The migration at %s does'+
                                            ' not exist') % self.filepath)
        
        #TODO - Really don't like this. exec is evil.
        f = codecs.open(self.filepath, "rU", "utf-8")
        exec(f.read())
        f.close()

        try:
            (SQL_UP, SQL_DOWN)
        except NameError:
            msg = ("Migration file at '%s' it not well-formed. " + \
                   "It should have both SQL_UP and SQL_DOWN variable " + \
                   "assignments.") % self.filepath
            raise InvalidMigrationFileError(msg)


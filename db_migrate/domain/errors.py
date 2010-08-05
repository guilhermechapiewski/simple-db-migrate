#!/usr/bin/env python
# encoding: utf-8
"""
Module responsible for all the typed errors in DbMigrate.
"""

class InvalidMigrationFilenameError(RuntimeError):
    '''
    Error that occurs whenever a migration to be parsed has an
    invalid name. 
    The names should be like 
    timestamp_whatever_this_migration_does.migration.
    '''
    pass

class MigrationFileDoesNotExistError(RuntimeError):
    '''
    Error that occurs whenever a migration tries to load a file 
    from the disk and doesn't find it.
    '''
    pass
class InvalidMigrationFileError(RuntimeError):
    '''
    Error that occurs whenever the contents of the migration file
    are not parsable, meaning there's a SQL_UP or SQL_DOWN variable missing.
    '''
    pass
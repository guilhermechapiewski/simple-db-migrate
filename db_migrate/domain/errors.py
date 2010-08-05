#!/usr/bin/env python
# encoding: utf-8
"""
Module responsible for all the typed errors in DbMigrate.
"""

class InvalidMigrationFilenameError(RuntimeError):
    pass
class MigrationFileDoesNotExistError(RuntimeError):
    pass
class InvalidMigrationFileError(RuntimeError):
    pass
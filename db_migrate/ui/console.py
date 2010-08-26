#!/usr/bin/env python
# encoding: utf-8

"""
The actual console infrastructure.
"""

import sys
from optparse import OptionParser

from db_migrate.ui.helper import Actions

class Console(object):
    '''
    Class responsible for controlling console interaction with users.
    '''

    def __init__(self):
        '''Initializes the console'''
        self.parser = None
        self.arguments = None
        self.options = None

    def run(self, arguments=None):
        '''Parses arguments and runs the required actions'''
        if arguments is None:
            arguments = sys.argv[1:]

        self.parse_arguments(arguments)

        if not self.arguments:
            action = "AutoMigrate"
        else:
            action = self.arguments[0]

    def parse_arguments(self, arguments):
        '''
        Parses the arguments and sets a dictionary of configurations.
        '''
        self.parser = OptionParser()

        self.parser.add_option("-c", "--config", 
                dest="config_file", 
                default="simple-db-migrate.conf", 
                help="""Use a specific config file. 
                If not provided, will search for 'simple-db-migrate.conf' 
                in the current directory.""")

        self.parser.add_option("-l", "--log-level", 
                dest="log_level", 
                default=1, 
                help="""Log level: 0-no log; 1-migrations log; 
2-statement execution log (default: %default)""")

        self.parser.add_option("-m", "--migration", 
                dest="schema_version", 
                default=None, 
                help="""Schema version to migrate to. If not provided will
migrate to the last version available in the migrations directory.""")

        self.parser.add_option("-n", "--create", "--new", 
                dest="new_migration", 
                default=None, 
                help="""Create migration file with the given nickname. The
 nickname should contain only lowercase characters and underscore '_'.
 Example: 'create_table_xyz'.""")

        self.parser.add_option("-p", "--paused-mode", 
                action="store_true", 
                dest="paused_mode", 
                default=False, 
                help="""Execute in 'paused' mode. In this mode you will need
 to press <enter> key in order to execute each SQL command, making it easier
 to see what is being executed and helping debug. When paused mode is enabled,
 log level is automatically set to [2].""")

        self.parser.add_option("-v", "--version", 
                action="store_true",
                dest="simple_db_migrate_version", 
                default=False, 
                help="Displays simple-db-migrate's version and exit.")

        self.parser.add_option("--color", 
                action="store_true", 
                dest="show_colors", 
                default=False, 
                help="Output with beautiful colors.")

        self.parser.add_option("--drop", "--drop-database-first",
                action="store_true", 
                dest="drop_db_first", 
                default=False, 
                help="""Drop database before running migrations to create
 everything from scratch. Useful when the database schema is corrupted and the
 migration scripts are not working.""")

        self.parser.add_option("--showsql", 
                action="store_true", 
                dest="show_sql", 
                default=False, 
                help="Show all SQL statements executed.")

        self.parser.add_option("--showsqlonly", 
                action="store_true", 
                dest="show_sql_only", 
                default=False, 
                help="""Show all SQL statements that would be executed but
 DON'T execute them in the database.""")

        (arguments, options) = self.parser.parse_args(arguments)
        
        self.arguments = arguments
        self.options = options

#-*- coding:utf-8 -*-

import os
import fnmatch
from optparse import make_option

from django import db
from django.conf import settings
from django.core.management.base import BaseCommand

import simple_db_migrate

class Command(BaseCommand):
    help = "Migrate databases."
    args = "[db_migrate_options]"

    option_list = BaseCommand.option_list + simple_db_migrate.cli.CLI.options_to_parser() + (
        make_option(
            '--database', action='store', dest='database',
            default=getattr(db, 'DEFAULT_DB_ALIAS', 'default'),
            help='Nominates a database to synchronize. Defaults to the "default" database.'
        ),
    )

    def handle(self, *args, **options):
        if not options.get('database_migrations_dir'):
            options['database_migrations_dir'] = Command._locate_migrations()

        for key in ['host', 'name', 'user', 'password']:
            options_key = 'database_' + key
            if options.get(options_key) == None:
                options[options_key] = Command._get_database_option(options, key)

        simple_db_migrate.run(options=options)

    @staticmethod
    def _get_database_option(options, key):
        # Handles Django 1.2+ database settings
        if hasattr(settings, 'DATABASES'):
            return settings.DATABASES[options.get('database')].get(key.upper(), '')
        # Fallback for Django 1.1 or lower
        return getattr(settings, 'DATABASE_' + key.upper(), None)

    @staticmethod
    def _locate_migrations():
        files = Command._locate_resource_dirs("migrations", "*.migration")

        if hasattr(settings, 'OTHER_MIGRATION_DIRS'):
            other_dirs = settings.OTHER_MIGRATION_DIRS
            if not isinstance(other_dirs, (tuple, list)):
                raise TypeError, 'The setting "OTHER_MIGRATION_DIRS" must be a tuple or a list'
            files.extend(other_dirs)

        return ':'.join(files)

    @staticmethod
    def _locate_resource_dirs(complement, pattern):
        dirs = []
        for app in settings.INSTALLED_APPS:
            fromlist = ""

            app_parts = app.split(".")
            if len(app_parts) > 1:
                fromlist = ".".join(app_parts[1:])

            module = __import__(app, fromlist=fromlist)
            app_dir = os.path.abspath("/" + "/".join(module.__file__.split("/")[1:-1]))

            resource_dir = os.path.join(app_dir, complement)

            if os.path.exists(resource_dir) and Command._locate_files(resource_dir, pattern):
                dirs.append(resource_dir)

        return dirs

    @staticmethod
    def _locate_files(root, pattern):
        return_files = []
        for path, dirs, files in os.walk(root):
            for filename in fnmatch.filter(files, pattern):
                return_files.append(os.path.join(path, filename))
        return return_files

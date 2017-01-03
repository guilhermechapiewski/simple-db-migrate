import unittest
import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.django_test_settings'
from django.core.management import call_command
from django.conf import settings
from simple_db_migrate.db_migrate.management.commands.dbmigrate import *
from mock import patch, Mock, MagicMock
from tests import BaseTest

@patch.dict('sys.modules', MySQLdb=MagicMock())
class DBMigrateTest(BaseTest):
    def setUp(self):
        super(DBMigrateTest, self).setUp()
        settings.INSTALLED_APPS = ['simple_db_migrate.db_migrate']
        for key in ['OTHER_MIGRATION_DIRS', 'DATABASES', 'DATABASE_HOST', 'DATABASE_NAME', 'DATABASE_USER', 'DATABASE_PASSWORD', 'DATABASE_PORT', 'DATABASE_ENGINE']:
            if hasattr(settings, key) and getattr(settings, key) is not None:
                delattr(settings, key)

    @patch('simple_db_migrate.run')
    def test_it_should_include_all_options_defined_on_the_command_line_version(self, run_mock):
        call_command('dbmigrate')
        options_keys = run_mock.call_args[1]['options'].keys()
        for option in simple_db_migrate.cli.CLI.options_to_parser():
            self.assertTrue(option['dest'] in options_keys, "'%s' not present in the options list" % option['dest'])

    @patch('simple_db_migrate.run')
    def test_it_should_include_the_database_option(self, run_mock):
        call_command('dbmigrate')
        options_keys = run_mock.call_args[1]['options'].keys()
        self.assertTrue('database' in options_keys, "'database' not present in the options list")

    @patch('simple_db_migrate.run')
    def test_it_should_use_the_specified_database_migrations_dir_value(self, run_mock):
        call_command('dbmigrate', database_migrations_dir='some_folder')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_migrations_dir'], 'some_folder')

    @patch('simple_db_migrate.run')
    def test_it_should_look_for_migrations_on_installed_apps_when_database_migrations_dir_is_not_specified_and_return_empty_if_not_found(self, run_mock):
        call_command('dbmigrate')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_migrations_dir'], '')

    @patch('simple_db_migrate.run')
    def test_it_should_look_for_migrations_on_installed_apps_when_database_migrations_dir_is_not_specified_and_return_a_list_of_apps_dir_with_migrations(self, run_mock):
        settings.INSTALLED_APPS.extend(['tests.apps.app1', 'tests.apps.app2', 'tests.apps.app3'])
        call_command('dbmigrate')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_migrations_dir'], '%s:%s' % (
                os.path.join(os.path.dirname(__file__), 'apps/app1/migrations'),
                os.path.join(os.path.dirname(__file__), 'apps/app3/migrations')
            )
        )

    @patch('simple_db_migrate.run')
    def test_it_should_append_the_folders_on_other_migration_dirs_settings(self, run_mock):
        settings.INSTALLED_APPS.extend(['tests.apps.app1', 'tests.apps.app2', 'tests.apps.app3'])
        settings.OTHER_MIGRATION_DIRS = ['some/folder1', 'some/folder2']
        call_command('dbmigrate')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_migrations_dir'], '%s:%s:%s:%s' % (
                os.path.join(os.path.dirname(__file__), 'apps/app1/migrations'),
                os.path.join(os.path.dirname(__file__), 'apps/app3/migrations'),
                'some/folder1',
                'some/folder2'
            )
        )

        settings.OTHER_MIGRATION_DIRS = ('some/folder3', 'some/folder4',)
        call_command('dbmigrate')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_migrations_dir'], '%s:%s:%s:%s' % (
                os.path.join(os.path.dirname(__file__), 'apps/app1/migrations'),
                os.path.join(os.path.dirname(__file__), 'apps/app3/migrations'),
                'some/folder3',
                'some/folder4'
            )
        )

    @patch('simple_db_migrate.run')
    def test_it_should_raise_exception_if_other_migration_dirs_settings_is_not_a_tuple_or_list(self, run_mock):
        settings.INSTALLED_APPS.extend(['tests.apps.app1', 'tests.apps.app2', 'tests.apps.app3'])
        settings.OTHER_MIGRATION_DIRS = 'some/folder1'
        self.assertRaisesWithMessage(TypeError, 'The setting "OTHER_MIGRATION_DIRS" must be a tuple or a list', call_command, 'dbmigrate')

    @patch('simple_db_migrate.run')
    def test_it_should_get_database_properties_from_settings_on_old_format(self, run_mock):
        settings.DATABASE_HOST = 'hostname1'
        settings.DATABASE_NAME = 'name1'
        settings.DATABASE_USER = 'user1'
        settings.DATABASE_PASSWORD = 'password1'
        settings.DATABASE_PORT = 1
        settings.DATABASE_ENGINE = 'engine1'
        call_command('dbmigrate')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_host'], 'hostname1')
        self.assertEquals(options['database_name'], 'name1')
        self.assertEquals(options['database_user'], 'user1')
        self.assertEquals(options['database_password'], 'password1')
        self.assertEquals(options['database_port'], 1)
        self.assertEquals(options['database_engine'], 'engine1')

    @patch('simple_db_migrate.run')
    def test_it_should_get_database_properties_from_settings_on_new_format(self, run_mock):
        settings.DATABASES = {
            'mydatabase' : { 'HOST' : 'hostname2', 'NAME': 'name2', 'USER': 'user2', 'PASSWORD': 'password2', 'PORT': 12, 'ENGINE': 'engine2' },
            'default' : { 'HOST' : 'hostname3', 'NAME': 'name3', 'USER': 'user3', 'PASSWORD': 'password3', 'PORT': 123, 'ENGINE': 'engine3' }
        }
        call_command('dbmigrate', database='mydatabase')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_host'], 'hostname2')
        self.assertEquals(options['database_name'], 'name2')
        self.assertEquals(options['database_user'], 'user2')
        self.assertEquals(options['database_password'], 'password2')
        self.assertEquals(options['database_port'], 12)
        self.assertEquals(options['database_engine'], 'engine2')

        call_command('dbmigrate')
        options = run_mock.call_args[1]['options']
        self.assertEquals(options['database_host'], 'hostname3')
        self.assertEquals(options['database_name'], 'name3')
        self.assertEquals(options['database_user'], 'user3')
        self.assertEquals(options['database_password'], 'password3')
        self.assertEquals(options['database_port'], 123)
        self.assertEquals(options['database_engine'], 'engine3')

if __name__ == "__main__":
    unittest.main()

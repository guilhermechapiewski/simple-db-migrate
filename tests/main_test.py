import unittest
import os
import glob
import re
from StringIO import StringIO
from mock import patch, call, Mock
from simple_db_migrate.main import *
from simple_db_migrate.core import Migration, SimpleDBMigrate
from simple_db_migrate.config import *
from tests import BaseTest, create_file, create_migration_file, delete_files, create_config

class MainTest(BaseTest):
    def setUp(self):
        self.initial_config = {
            'database_host': 'localhost',
            'database_name': 'test',
            'database_user': 'user',
            'database_password': 'password',
            'database_migrations_dir': ['.'],
            'database_engine': 'engine',
            'schema_version': None
        }
        if not os.path.exists(os.path.abspath('migrations')):
            os.mkdir(os.path.abspath('migrations'))
        self.test_migration_files = []

        self.test_migration_files.append(os.path.abspath(create_migration_file('20090214115100_01_test_migration.migration', 'foo 1', 'bar 1')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('migrations/20090214115200_02_test_migration.migration', 'foo 2', 'bar 2')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('20090214115300_03_test_migration.migration', 'foo 3', 'bar 3')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('20090214115400_04_test_migration.migration', 'foo 4', 'bar 4')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('migrations/20090214115500_05_test_migration.migration', 'foo 5', 'bar 5')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('migrations/20090214115600_06_test_migration.migration', 'foo 6', 'bar 6')))

    def test_it_should_raise_error_if_a_required_config_to_migrate_is_missing(self):
        self.assertRaisesWithMessage(Exception, "invalid key ('database_host')", Main, config=Config())
        self.assertRaisesWithMessage(Exception, "invalid key ('database_name')", Main, config=Config({'database_host': ''}))
        self.assertRaisesWithMessage(Exception, "invalid key ('database_user')", Main, config=Config({'database_host': '', 'database_name': ''}))
        self.assertRaisesWithMessage(Exception, "invalid key ('database_password')", Main, config=Config({'database_host': '', 'database_name': '', 'database_user': ''}))
        self.assertRaisesWithMessage(Exception, "invalid key ('database_migrations_dir')", Main, config=Config({'database_host': '', 'database_name': '', 'database_user': '', 'database_password': ''}))
        self.assertRaisesWithMessage(Exception, "invalid key ('database_engine')", Main, config=Config({'database_host': '', 'database_name': '', 'database_user': '', 'database_password': '', 'database_migrations_dir': ''}))
        self.assertRaisesWithMessage(Exception, "invalid key ('schema_version')", Main, config=Config({'database_host': '', 'database_name': '', 'database_user': '', 'database_password': '', 'database_migrations_dir': '', 'database_engine':''}))

    def test_it_should_raise_error_if_a_required_config_to_create_migration_is_missing(self):
        self.assertRaisesWithMessage(Exception, "invalid key ('database_migrations_dir')", Main, config=Config({'new_migration': 'new'}))
        try:
            Main(Config({'new_migration': 'new', 'database_migrations_dir': ''}))
        except:
            self.fail("it should not get here")

    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG')
    @patch('simple_db_migrate.main.CLI')
    def test_it_should_use_the_other_utilities_classes(self, cli_mock, log_mock, simpledbmigrate_mock):
        config = Config(self.initial_config)
        Main(sgdb=Mock(), config=config)
        self.assertEqual(1, cli_mock.call_count)
        log_mock.assert_called_with(None)
        simpledbmigrate_mock.assert_called_with(config)

    @patch('simple_db_migrate.main.LOG')
    def test_it_should_use_log_dir_from_config(self, log_mock):
        self.initial_config.update({'log_dir':'.', "database_migrations_dir":['.']})
        Main(sgdb=Mock(), config=Config(self.initial_config))
        log_mock.assert_called_with('.')

    @patch('simple_db_migrate.mysql.MySQL')
    def test_it_should_use_mysql_class_if_choose_this_engine(self, mysql_mock):
        self.initial_config.update({'log_dir':'.', 'database_engine': 'mysql', "database_migrations_dir":['.']})
        config=Config(self.initial_config)
        Main(config=config)
        mysql_mock.assert_called_with(config)

    @patch('simple_db_migrate.oracle.Oracle')
    def test_it_should_use_oracle_class_if_choose_this_engine(self, oracle_mock):
        self.initial_config.update({'log_dir':'.', 'database_engine': 'oracle', "database_migrations_dir":['.']})
        config=Config(self.initial_config)
        Main(config=config)
        oracle_mock.assert_called_with(config)

    @patch('simple_db_migrate.mssql.MSSQL')
    def test_it_should_use_mssql_class_if_choose_this_engine(self, mssql_mock):
        self.initial_config.update({'log_dir':'.', 'database_engine': 'mssql', "database_migrations_dir":['.']})
        config=Config(self.initial_config)
        Main(config=config)
        mssql_mock.assert_called_with(config)

    def test_it_should_raise_error_if_config_is_not_an_instance_of_simple_db_migrate_config(self):
        self.assertRaisesWithMessage(Exception, "config must be an instance of simple_db_migrate.config.Config", Main, config={})

    def test_it_should_raise_error_if_choose_an_invalid_engine(self):
        self.initial_config.update({'log_dir':'.', 'database_engine': 'invalid_engine'})
        config=Config(self.initial_config)
        self.assertRaisesWithMessage(Exception, "engine not supported 'invalid_engine'", Main, config=config)

    def test_it_should_ignore_engine_configuration_if_asked_to_create_a_new_migration(self):
        self.initial_config.update({'new_migration':'new_test_migration', 'database_engine': 'invalid_engine', "database_migrations_dir":['.']})
        config=Config(self.initial_config)
        try:
            Main(config)
        except:
            self.fail("it should not get here")

    @patch('simple_db_migrate.main.Main._execution_log')
    @patch('simple_db_migrate.main.Migration.create', return_value='created_file')
    def test_it_should_create_migration_if_option_is_activated_by_the_user(self, migration_mock, _execution_log_mock):
        self.initial_config.update({'new_migration':'new_test_migration', 'database_engine': 'invalid_engine', "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call("- Created file 'created_file'", log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        migration_mock.assert_called_with('new_test_migration', 'migrations', 'utf-8', False)

    @patch('simple_db_migrate.main.Migration.create', return_value='created_file')
    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG')
    @patch('simple_db_migrate.main.CLI')
    def test_it_should_create_new_migration_with_utc_timestamp(self, cli_mock, log_mock, simpledbmigrate_mock, migration_mock):
        self.initial_config.update({'new_migration':'new_test_migration', 'database_engine': 'invalid_engine', "database_migrations_dir":['migrations', '.'], 'utc_timestamp': True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(), config=config)
        main.execute()
        migration_mock.assert_called_with('new_test_migration', 'migrations', 'utf-8', True)

    @patch('simple_db_migrate.main.Migration.create', return_value='created_file')
    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG')
    @patch('simple_db_migrate.main.CLI')
    def test_it_should_create_new_migration_with_different_encoding(self, cli_mock, log_mock, simpledbmigrate_mock, migration_mock):
        self.initial_config.update({'new_migration':'new_test_migration', 'database_engine': 'invalid_engine', "database_migrations_dir":['migrations', '.'], 'database_script_encoding': 'iso8859-1'})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(), config=config)
        main.execute()
        migration_mock.assert_called_with('new_test_migration', 'migrations', 'iso8859-1', False)

    @patch('simple_db_migrate.main.Main._execution_log')
    @patch('simple_db_migrate.main.Main._migrate')
    def test_it_should_migrate_db_if_create_migration_option_is_not_activated_by_user(self, migrate_mock, _execution_log_mock):
        config=Config(self.initial_config)
        main = Main(config=config, sgdb=Mock())
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        self.assertEqual(1, migrate_mock.call_count)

    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG.debug')
    @patch('simple_db_migrate.main.CLI')
    def test_it_should_write_the_message_to_log(self, cli_mock, log_mock, simpledbmigrate_mock):
        main = Main(sgdb=Mock(), config=Config(self.initial_config))
        main._execution_log('message to log')

        log_mock.assert_called_with('message to log')

    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG')
    @patch('simple_db_migrate.main.CLI.msg')
    def test_it_should_write_the_message_to_cli(self, cli_mock, log_mock, simpledbmigrate_mock):
        main = Main(sgdb=Mock(), config=Config(self.initial_config))
        main._execution_log('message to log', color='RED', log_level_limit=1)

        cli_mock.assert_called_with('message to log', 'RED')

    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG')
    @patch('simple_db_migrate.main.CLI.msg')
    def test_it_should_write_the_message_to_cli_using_default_color(self, cli_mock, log_mock, simpledbmigrate_mock):
        self.initial_config.update({'log_level':3})
        main = Main(sgdb=Mock(), config=Config(self.initial_config))
        main._execution_log('message to log')

        cli_mock.assert_called_with('message to log', 'CYAN')

    @patch('simple_db_migrate.main.Main._execute_migrations')
    @patch('simple_db_migrate.main.Main._get_destination_version', return_value='destination_version')
    @patch('simple_db_migrate.main.SimpleDBMigrate')
    @patch('simple_db_migrate.main.LOG')
    @patch('simple_db_migrate.main.CLI')
    def test_it_should_get_current_and_destination_versions_and_execute_migrations(self, cli_mock, log_mock, simpledbmigrate_mock, _get_destination_version_mock, execute_migrations_mock):
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'current_schema_version'}), config=Config(self.initial_config))
        main.execute()
        execute_migrations_mock.assert_called_with('current_schema_version', 'destination_version')

    def test_it_should_get_destination_version_when_user_informs_a_specific_version(self):
        self.initial_config.update({"schema_version":"20090214115300", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_id_from_version_number.return_value':None}), config=config)
        self.assertEqual("20090214115300", main._get_destination_version())
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_get_destination_version_when_user_does_not_inform_a_specific_version(self):
        self.initial_config.update({"database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(), config=config)
        self.assertEqual("20090214115600", main._get_destination_version())

    def test_it_should_raise_exception_when_get_destination_version_and_version_does_not_exist_on_database_or_on_migrations_dir(self):
        self.initial_config.update({"schema_version":"20090214115900", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_id_from_version_number.return_value':None}), config=config)
        self.assertRaisesWithMessage(Exception, 'version not found (20090214115900)', main.execute)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115900')
        self.assertEqual(2, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_get_destination_version_when_user_informs_a_label_and_it_does_not_exists_in_database(self):
        self.initial_config.update({"label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':None}), config=config)
        self.assertEqual("20090214115600", main._get_destination_version())
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')

    def test_it_should_get_destination_version_when_user_informs_a_specific_version_and_it_exists_on_database(self):
        self.initial_config.update({"schema_version":"20090214115300", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_id_from_version_number.return_value':3}), config=config)
        self.assertEqual("20090214115300", main._get_destination_version())
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_get_destination_version_when_user_informs_a_label_and_a_version_and_it_does_not_exists_in_database(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':None, 'get_version_id_from_version_number.return_value':None}), config=config)
        self.assertEqual("20090214115300", main._get_destination_version())
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)


    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[])
    def test_it_should_do_migration_down_if_a_label_and_a_version_were_specified_and_both_of_them_are_present_at_database_and_correspond_to_same_migration(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115600', 'get_version_number_from_label.return_value':'20090214115300', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115300', False)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[])
    def test_it_should_do_migration_down_if_a_label_and_a_version_were_specified_and_both_of_them_are_present_at_database_and_correspond_to_same_migration_and_force_execute_old_migrations_versions_is_set(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.'], "force_execute_old_migrations_versions": True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115600', 'get_version_number_from_label.return_value':'20090214115300', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115300', False)

    def test_it_should_get_destination_version_and_update_config_when_user_informs_a_label_and_it_exists_in_database(self):
        self.initial_config.update({"schema_version":None, "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':'20090214115300', 'get_version_id_from_version_number.return_value':3}), config=config)
        self.assertEqual("20090214115300", main._get_destination_version())
        self.assertEqual("20090214115300", config.get("schema_version"))
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_raise_exception_when_get_destination_version_and_version_and_label_point_to_a_different_migration_on_database(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':'20090214115400', 'get_version_id_from_version_number.return_value':3}), config=config)
        self.assertRaisesWithMessage(Exception, "label (test_label) and schema_version (20090214115300) don't correspond to the same version at database", main.execute)
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_raise_exception_when_get_destination_version_and_version_exists_on_database_and_label_not(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':None, 'get_version_id_from_version_number.return_value':3}), config=config)
        self.assertRaisesWithMessage(Exception, "label (test_label) or schema_version (20090214115300), only one of them exists in the database", main.execute)
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_raise_exception_when_get_destination_version_and_label_exists_on_database_and_version_not(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':'20090214115400', 'get_version_id_from_version_number.return_value':None}), config=config)
        self.assertRaisesWithMessage(Exception, "label (test_label) or schema_version (20090214115300), only one of them exists in the database", main.execute)
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_raise_exception_when_get_destination_version_and_version_and_label_point_to_a_different_migration_on_database_and_force_execute_old_migrations_versions_is_set(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.'], "force_execute_old_migrations_versions":True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':'20090214115400', 'get_version_id_from_version_number.return_value':3}), config=config)
        self.assertRaisesWithMessage(Exception, "label (test_label) and schema_version (20090214115300) don't correspond to the same version at database", main.execute)
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_raise_exception_when_get_destination_version_and_version_exists_on_database_and_label_not_and_force_execute_old_migrations_versions_is_set(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.'], "force_execute_old_migrations_versions":True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':None, 'get_version_id_from_version_number.return_value':3}), config=config)
        self.assertRaisesWithMessage(Exception, "label (test_label) or schema_version (20090214115300), only one of them exists in the database", main.execute)
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    def test_it_should_raise_exception_when_get_destination_version_and_label_exists_on_database_and_version_not_and_force_execute_old_migrations_versions_is_set(self):
        self.initial_config.update({"schema_version":"20090214115300", "label_version":"test_label", "database_migrations_dir":['migrations', '.'], "force_execute_old_migrations_versions":True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_version_number_from_label.return_value':'20090214115400', 'get_version_id_from_version_number.return_value':None}), config=config)
        self.assertRaisesWithMessage(Exception, "label (test_label) or schema_version (20090214115300), only one of them exists in the database", main.execute)
        main.sgdb.get_version_number_from_label.assert_called_with('test_label')
        self.assertEqual(1, main.sgdb.get_version_number_from_label.call_count)
        main.sgdb.get_version_id_from_version_number.assert_called_with('20090214115300')
        self.assertEqual(1, main.sgdb.get_version_id_from_version_number.call_count)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[])
    def test_it_should_migrate_database_with_migration_is_up(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version": None, "label_version": None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115300', 'get_version_id_from_version_number.return_value':None}), config=config)
        main.execute()
        files_to_be_executed_mock.assert_called_with('20090214115300', '20090214115600', True)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[])
    def test_it_should_migrate_database_with_migration_is_down_when_specify_a_version_older_than_that_on_database(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version": '20090214115200', "label_version": None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115300', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()
        files_to_be_executed_mock.assert_called_with('20090214115300', '20090214115200', False)

    def test_it_should_raise_error_when_specify_a_version_older_than_the_current_database_version_and_is_not_present_on_database(self):
        self.initial_config.update({"schema_version": '20090214115100', "label_version": None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115300', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'Trying to migrate to a lower version wich is not found on database (20090214115100)', main.execute)

    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_just_log_message_when_dont_have_any_migration_to_execute(self, _execution_log_mock):
        self.initial_config.update({"schema_version": None, "label_version": None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.return_value':None}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('\nNothing to do.\n', 'PINK', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[])
    def test_it_should_do_migration_down_if_a_label_was_specified_and_a_version_was_not_specified_and_label_is_present_at_database(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":None, "label_version":"test_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115600', 'get_version_number_from_label.return_value':'20090214115300', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115300', False)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[])
    def test_it_should_do_migration_down_if_a_label_was_specified_and_a_version_was_not_specified_and_label_is_present_at_database_and_force_execute_old_migrations_versions_is_set(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":None, "label_version":"test_label", "database_migrations_dir":['migrations', '.'], "force_execute_old_migrations_versions":True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115600', 'get_version_number_from_label.return_value':'20090214115300', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115300', False)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05"), Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_only_log_sql_commands_when_show_sql_only_is_set_and_is_up(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115600', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql_only':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115600', 'GREEN', log_level_limit=1),
            call("\nWARNING: database migrations are not being executed ('--showsqlonly' activated)", 'YELLOW', log_level_limit=1),
            call("*** versions: ['20090214115500', '20090214115600']\n", 'CYAN', log_level_limit=1),
            call('__________ SQL statements executed __________', 'YELLOW', log_level_limit=1),
            call('sql up 05', 'YELLOW', log_level_limit=1),
            call('sql up 06', 'YELLOW', log_level_limit=1),
            call('_____________________________________________', 'YELLOW', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115400', '20090214115600', True)
        self.assertEqual(0, main.sgdb.change.call_count)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06"), Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_only_log_sql_commands_when_show_sql_only_is_set_and_is_down(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115400', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql_only':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115400', 'GREEN', log_level_limit=1),
            call("\nWARNING: database migrations are not being executed ('--showsqlonly' activated)", 'YELLOW', log_level_limit=1),
            call("*** versions: ['20090214115600', '20090214115500']\n", 'CYAN', log_level_limit=1),
            call('__________ SQL statements executed __________', 'YELLOW', log_level_limit=1),
            call('sql down 06', 'YELLOW', log_level_limit=1),
            call('sql down 05', 'YELLOW', log_level_limit=1),
            call('_____________________________________________', 'YELLOW', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115400', False)
        self.assertEqual(0, main.sgdb.change.call_count)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05"), Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_execute_sql_commands_when_show_sql_only_is_not_set_and_is_up(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115600', "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('\nStarting migration up!', log_level_limit=1),
            call("*** versions: ['20090214115500', '20090214115600']\n", 'CYAN', log_level_limit=1),
            call('===== executing 20090214115500_05_test_migration.migration (up) =====', log_level_limit=1),
            call('===== executing 20090214115600_06_test_migration.migration (up) =====', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115400', '20090214115600', True)
        expected_calls = [
            call('sql up 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', True, _execution_log_mock, None),
            call('sql up 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', True, _execution_log_mock, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06"), Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_execute_sql_commands_when_show_sql_only_is_not_set_and_is_down(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115400', "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('\nStarting migration down!', log_level_limit=1),
            call("*** versions: ['20090214115600', '20090214115500']\n", 'CYAN', log_level_limit=1),
            call('===== executing 20090214115600_06_test_migration.migration (down) =====', log_level_limit=1),
            call('===== executing 20090214115500_05_test_migration.migration (down) =====', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115400', False)
        expected_calls = [
            call('sql down 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', False, _execution_log_mock, None),
            call('sql down 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', False, _execution_log_mock, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05"), Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_execute_and_log_sql_commands_when_show_sql_is_set_and_is_up(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115600', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('\nStarting migration up!', log_level_limit=1),
            call("*** versions: ['20090214115500', '20090214115600']\n", 'CYAN', log_level_limit=1),
            call('===== executing 20090214115500_05_test_migration.migration (up) =====', log_level_limit=1),
            call('===== executing 20090214115600_06_test_migration.migration (up) =====', log_level_limit=1),
            call('__________ SQL statements executed __________', 'YELLOW', log_level_limit=1),
            call('sql up 05', 'YELLOW', log_level_limit=1),
            call('sql up 06', 'YELLOW', log_level_limit=1),
            call('_____________________________________________', 'YELLOW', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115400', '20090214115600', True)
        expected_calls = [
            call('sql up 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', True, _execution_log_mock, None),
            call('sql up 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', True, _execution_log_mock, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06"), Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_execute_and_log_sql_commands_when_show_sql_is_set_and_is_down(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115400', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('\nStarting migration down!', log_level_limit=1),
            call("*** versions: ['20090214115600', '20090214115500']\n", 'CYAN', log_level_limit=1),
            call('===== executing 20090214115600_06_test_migration.migration (down) =====', log_level_limit=1),
            call('===== executing 20090214115500_05_test_migration.migration (down) =====', log_level_limit=1),
            call('__________ SQL statements executed __________', 'YELLOW', log_level_limit=1),
            call('sql down 06', 'YELLOW', log_level_limit=1),
            call('sql down 05', 'YELLOW', log_level_limit=1),
            call('_____________________________________________', 'YELLOW', log_level_limit=1),
            call('\nDone.\n', 'PINK', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115400', False)
        expected_calls = [
            call('sql down 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', False, _execution_log_mock, None),
            call('sql down 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', False, _execution_log_mock, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05"), Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06")])
    def test_it_should_apply_label_to_executed_sql_commands_when_a_label_was_specified_and_is_up(self, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":None, "label_version":"new_label", "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_version_number_from_label.return_value':None, 'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        expected_calls = [
            call('sql up 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', True, main._execution_log, 'new_label'),
            call('sql up 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', True, main._execution_log, 'new_label')
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05"), Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_raise_exception_and_stop_process_when_an_error_occur_on_executing_sql_commands_and_is_up(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115600', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.side_effect':Exception('error when executin sql'), 'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'error when executin sql', main.execute)

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('\nStarting migration up!', log_level_limit=1),
            call("*** versions: ['20090214115500', '20090214115600']\n", 'CYAN', log_level_limit=1),
            call('===== executing 20090214115500_05_test_migration.migration (up) =====', log_level_limit=1),
            call('===== ERROR executing  (up) =====', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115400', '20090214115600', True)
        expected_calls = [
            call('sql up 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', True, _execution_log_mock, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06"), Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05")])
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_raise_exception_and_stop_process_when_an_error_occur_on_executing_sql_commands_and_is_down(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115400', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.side_effect':Exception('error when executin sql'), 'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'error when executin sql', main.execute)

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
            call('- Current version is: 20090214115600', 'GREEN', log_level_limit=1),
            call('- Destination version is: 20090214115400', 'GREEN', log_level_limit=1),
            call('\nStarting migration down!', log_level_limit=1),
            call("*** versions: ['20090214115600', '20090214115500']\n", 'CYAN', log_level_limit=1),
            call('===== executing 20090214115600_06_test_migration.migration (down) =====', log_level_limit=1),
            call('===== ERROR executing  (down) =====', log_level_limit=1)
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115400', False)
        expected_calls = [
            call('sql down 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', False, _execution_log_mock, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', side_effect=Exception('error getting migrations to execute'))
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_raise_exception_and_stop_process_when_an_error_occur_on_getting_migrations_to_execute_and_is_up(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115600', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'error getting migrations to execute', main.execute)

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115400', '20090214115600', True)
        expected_calls = [
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', side_effect=Exception('error getting migrations to execute'))
    @patch('simple_db_migrate.main.Main._execution_log')
    def test_it_should_raise_exception_and_stop_process_when_an_error_occur_on_getting_migrations_to_execute_and_is_down(self, _execution_log_mock, files_to_be_executed_mock):
        self.initial_config.update({"schema_version":'20090214115400', "label_version":None, "database_migrations_dir":['migrations', '.'], 'show_sql':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'error getting migrations to execute', main.execute)

        expected_calls = [
            call('\nStarting DB migration...', 'PINK', log_level_limit=1),
        ]
        self.assertEqual(expected_calls, _execution_log_mock.mock_calls)
        files_to_be_executed_mock.assert_called_with('20090214115600', '20090214115400', False)
        expected_calls = [
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('sys.stdin', return_value="\n", **{'readline.return_value':"\n"})
    @patch('sys.stdout', new_callable=StringIO)
    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05"), Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06")])
    def test_it_should_pause_execution_after_each_migration_when_paused_mode_is_set_and_is_up(self, files_to_be_executed_mock, stdout_mock, stdin_mock):
        self.initial_config.update({"schema_version":'20090214115600', "label_version":None, "database_migrations_dir":['migrations', '.'], 'paused_mode':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115400', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        self.assertEqual(2, stdout_mock.getvalue().count("* press <enter> to continue..."))

        expected_calls = [
            call('sql up 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', True, main._execution_log, None),
            call('sql up 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', True, main._execution_log, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    @patch('sys.stdin', return_value="\n", **{'readline.return_value':"\n"})
    @patch('sys.stdout', new_callable=StringIO)
    @patch('simple_db_migrate.main.Main._get_migration_files_to_be_executed', return_value=[Migration(file_name="20090214115600_06_test_migration.migration", version="20090214115600", sql_up="sql up 06", sql_down="sql down 06"), Migration(file_name="20090214115500_05_test_migration.migration", version="20090214115500", sql_up="sql up 05", sql_down="sql down 05")])
    def test_it_should_pause_execution_after_each_migration_when_paused_mode_is_set_and_is_down(self, files_to_be_executed_mock, stdout_mock, stdin_mock):
        self.initial_config.update({"schema_version":'20090214115400', "label_version":None, "database_migrations_dir":['migrations', '.'], 'paused_mode':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'change.return_value':None, 'get_current_schema_version.return_value':'20090214115600', 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        main.execute()

        self.assertEqual(2, stdout_mock.getvalue().count("* press <enter> to continue..."))

        expected_calls = [
            call('sql down 06', '20090214115600', '20090214115600_06_test_migration.migration', 'sql up 06', 'sql down 06', False, main._execution_log, None),
            call('sql down 05', '20090214115500', '20090214115500_05_test_migration.migration', 'sql up 05', 'sql down 05', False, main._execution_log, None)
        ]
        self.assertEqual(expected_calls, main.sgdb.change.mock_calls)

    def test_it_should_return_an_empty_list_of_files_to_execute_if_current_and_destiny_version_are_equals(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_all_schema_versions.return_value':['20090214115100', '20090214115200', '20090214115300', '20090214115600']}), config=config)
        self.assertEqual([], main._get_migration_files_to_be_executed('20090214115600', '20090214115600', True))

    def test_it_should_return_an_empty_list_of_files_to_execute_if_current_and_destiny_version_are_equals_and_has_new_files_to_execute(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_all_schema_versions.return_value':['20090214115100', '20090214115200', '20090214115300']}), config=config)
        self.assertEqual([], main._get_migration_files_to_be_executed('20090214115300', '20090214115300', True))

    def test_it_should_check_if_has_any_old_files_to_execute_if_current_and_destiny_version_are_equals_and_force_old_migrations_is_set(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.'], 'force_execute_old_migrations_versions':True})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_all_schema_versions.return_value':['20090214115100', '20090214115200', '20090214115300', '20090214115600']}), config=config)
        migrations = main._get_migration_files_to_be_executed('20090214115600', '20090214115600', True)

        self.assertEqual(2, len(migrations))
        self.assertEqual('20090214115400_04_test_migration.migration', migrations[0].file_name)
        self.assertEqual('20090214115500_05_test_migration.migration', migrations[1].file_name)

    def test_it_should_check_if_has_any_old_files_to_execute_if_current_and_destiny_version_are_different(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        main = Main(sgdb=Mock(**{'get_all_schema_versions.return_value':['20090214115100', '20090214115300']}), config=config)
        migrations = main._get_migration_files_to_be_executed('20090214115300', '20090214115500', True)

        self.assertEqual(3, len(migrations))
        self.assertEqual('20090214115200_02_test_migration.migration', migrations[0].file_name)
        self.assertEqual('20090214115400_04_test_migration.migration', migrations[1].file_name)
        self.assertEqual('20090214115500_05_test_migration.migration', migrations[2].file_name)

    def test_it_should_return_an_empty_list_of_files_to_execute_if_current_and_destiny_version_are_equals_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115300_03_test_migration.migration", version="20090214115300", sql_up="sql up 03", sql_down="sql down 03", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115300', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertEqual([], main._get_migration_files_to_be_executed('20090214115400', '20090214115400', False))

    def test_it_should_get_all_schema_migrations_to_check_wich_one_has_to_be_removed_if_current_and_destiny_version_are_equals_and_is_down_and_force_old_migrations_is_set(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.'], 'force_execute_old_migrations_versions':True})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115300_03_test_migration.migration", version="20090214115300", sql_up="sql up 03", sql_down="sql down 03", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115300', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertEqual([], main._get_migration_files_to_be_executed('20090214115400', '20090214115400', False))

    def test_it_should_get_all_schema_migrations_to_check_wich_one_has_to_be_removed_if_current_and_destiny_version_are_different_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115300_03_test_migration.migration", version="20090214115300", sql_up="sql up 03", sql_down="sql down 03", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115300', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertEqual([all_schema_migrations[-1], all_schema_migrations[-2]], main._get_migration_files_to_be_executed('20090214115400', '20090214115200', False))

    def test_it_should_get_all_schema_migrations_to_check_wich_one_has_to_be_removed_if_one_of_migration_file_does_not_exists_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115301_03_test_migration.migration", version="20090214115301", sql_up="sql up 03.1", sql_down="sql down 03.1", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115301', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertEqual([all_schema_migrations[-1], all_schema_migrations[-2]], main._get_migration_files_to_be_executed('20090214115400', '20090214115200', False))

    def test_it_should_get_sql_down_from_file_if_sql_down_is_empty_on_database_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115300_03_test_migration.migration", version="20090214115300", sql_up="sql up 03", sql_down="", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115300', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        migrations = main._get_migration_files_to_be_executed('20090214115400', '20090214115200', False)
        self.assertEqual([all_schema_migrations[-1], all_schema_migrations[-2]], migrations)
        self.assertEqual(u"sql down 04", migrations[0].sql_down)
        self.assertEqual(u"bar 3", migrations[1].sql_down)

    def test_it_should_get_sql_down_from_file_if_force_use_files_is_set_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.'], 'force_use_files_on_down':True})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115300_03_test_migration.migration", version="20090214115300", sql_up="sql up 03", sql_down="sql down 03", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115300', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        migrations = main._get_migration_files_to_be_executed('20090214115400', '20090214115200', False)
        self.assertEqual([all_schema_migrations[-1], all_schema_migrations[-2]], migrations)
        self.assertEqual(u"bar 4", migrations[0].sql_down)
        self.assertEqual(u"bar 3", migrations[1].sql_down)

    def test_it_should_raise_exception_and_stop_process_when_a_migration_has_an_empty_sql_down_and_migration_file_is_not_present_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.']})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115301_03_test_migration.migration", version="20090214115301", sql_up="sql up 03.1", sql_down="", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115301', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'impossible to migrate down: one of the versions was not found (20090214115301)', main._get_migration_files_to_be_executed, '20090214115400', '20090214115200', False)

    def test_it_should_raise_exception_and_stop_process_when_a_migration_file_is_not_present_and_force_files_is_set_and_is_down(self):
        self.initial_config.update({"schema_version":None, "label_version":None, "database_migrations_dir":['migrations', '.'], 'force_use_files_on_down':True})
        config=Config(self.initial_config)
        all_schema_migrations = [
            Migration(file_name="20090214115200_02_test_migration.migration", version="20090214115200", sql_up="sql up 02", sql_down="sql down 02", id=2),
            Migration(file_name="20090214115301_03_test_migration.migration", version="20090214115301", sql_up="sql up 03.1", sql_down="sql down 03.1", id=3),
            Migration(file_name="20090214115300_04_test_migration.migration", version="20090214115400", sql_up="sql up 04", sql_down="sql down 04", id=4)
        ]
        main = Main(sgdb=Mock(**{'get_all_schema_migrations.return_value':all_schema_migrations, 'get_all_schema_versions.return_value':['20090214115200', '20090214115301', '20090214115400'], 'get_version_id_from_version_number.side_effect':get_version_id_from_version_number_side_effect}), config=config)
        self.assertRaisesWithMessage(Exception, 'impossible to migrate down: one of the versions was not found (20090214115301)', main._get_migration_files_to_be_executed, '20090214115400', '20090214115200', False)

def get_version_id_from_version_number_side_effect(args):
    if str(args) == '20090214115100':
        return None
    match = re.match("[0-9]{11}([0-9])[0-9]{2}", str(args))
    return int(match.group(1))

if __name__ == "__main__":
    unittest.main()

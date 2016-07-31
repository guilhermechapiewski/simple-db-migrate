import unittest
import simple_db_migrate
import os
import sys
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from mock import patch, Mock, MagicMock

try:
    from importlib import reload
except ImportError: pass

class RunTest(unittest.TestCase):

    def setUp(self):
        config_file = '''
DATABASE_HOST = os.getenv('DB_HOST') or 'localhost'
DATABASE_USER = os.getenv('DB_USERNAME') or 'root'
DATABASE_PASSWORD = os.getenv('DB_PASSWORD') or ''
DATABASE_NAME = os.getenv('DB_DATABASE') or 'migration_example'
ENV1_DATABASE_NAME = 'migration_example_env1'
DATABASE_MIGRATIONS_DIR = os.getenv('DATABASE_MIGRATIONS_DIR') or 'example'
UTC_TIMESTAMP = os.getenv("UTC_TIMESTAMP") or True
DATABASE_ANY_CUSTOM_VARIABLE = 'Some Value'
SOME_ENV_DATABASE_ANY_CUSTOM_VARIABLE = 'Other Value'
DATABASE_OTHER_CUSTOM_VARIABLE = 'Value'
'''
        f = open('sample.conf', 'w')
        f.write(config_file)
        f.close()
        self.stdout_mock = patch('sys.stdout', new_callable=StringIO)
        self.stdout_mock.start()

    def tearDown(self):
        os.remove('sample.conf')
        if os.path.exists('simple-db-migrate.conf'):
            os.remove('simple-db-migrate.conf')
        self.stdout_mock.stop()

    @patch('codecs.getwriter')
    @patch('sys.stdout', encoding='iso-8859-1')
    def test_it_should_ensure_stdout_is_using_an_utf8_encoding(self, stdout_mock, codecs_mock):
        new_stdout = Mock()
        codecs_mock.return_value = Mock(**{'return_value':new_stdout})

        reload(simple_db_migrate)

        codecs_mock.assert_called_with('utf-8')
        self.assertEqual(new_stdout, sys.stdout)

    @patch('sys.stdout', new_callable=object)
    def test_it_should_not_break_when_sys_stdout_has_not_encoding_property(self, stdout_mock):
        reload(simple_db_migrate)
        self.assertIs(stdout_mock, sys.stdout)

    def test_it_should_define_a_version_string(self):
        self.assertTrue(isinstance(simple_db_migrate.SIMPLE_DB_MIGRATE_VERSION, str))

    @patch('simple_db_migrate.cli.CLI.parse')
    def test_it_should_use_cli_to_parse_arguments(self, parse_mock):
        parse_mock.return_value = (Mock(simple_db_migrate_version=True), [])
        try:
            simple_db_migrate.run_from_argv()
        except SystemExit:
            pass

        parse_mock.assert_called_with(None)

    def test_it_should_print_simple_db_migrate_version_and_exit(self):
        try:
            simple_db_migrate.run_from_argv(["-v"])
        except SystemExit as e:
            self.assertEqual(0, e.code)

        self.assertEqual('simple-db-migrate v%s\n\n' % simple_db_migrate.SIMPLE_DB_MIGRATE_VERSION, sys.stdout.getvalue())

    @patch('simple_db_migrate.cli.CLI.show_colors')
    def test_it_should_activate_use_of_colors(self, show_colors_mock):
        try:
            simple_db_migrate.run_from_argv(["--color"])
        except SystemExit:
            pass

        self.assertEqual(1, show_colors_mock.call_count)

    @patch('simple_db_migrate.cli.CLI.show_colors')
    def test_it_should_print_message_and_exit_when_user_interrupt_execution(self, show_colors_mock):
        show_colors_mock.side_effect = KeyboardInterrupt()
        try:
            simple_db_migrate.run_from_argv(["--color"])
        except SystemExit as e:
            self.assertEqual(0, e.code)

        self.assertEqual('\nExecution interrupted by user...\n\n', sys.stdout.getvalue())

    @patch('simple_db_migrate.cli.CLI.show_colors')
    def test_it_should_print_message_and_exit_when_user_an_error_happen(self, show_colors_mock):
        show_colors_mock.side_effect = Exception('occur an error')
        try:
            simple_db_migrate.run_from_argv(["--color"])
        except SystemExit as e:
            self.assertEqual(1, e.code)

        self.assertEqual('[ERROR] occur an error\n\n', sys.stdout.getvalue())

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    @patch.object(simple_db_migrate.helpers.Utils, 'get_variables_from_file', return_value = {'DATABASE_HOST':'host', 'DATABASE_PORT':'1234', 'DATABASE_USER': 'root', 'DATABASE_PASSWORD':'', 'DATABASE_NAME':'database', 'DATABASE_MIGRATIONS_DIR':'.'})
    def test_it_should_read_configuration_file_using_fileconfig_class_and_execute_with_default_configuration(self, get_variables_from_file_mock, main_mock, execute_mock):
        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf')])

        get_variables_from_file_mock.assert_called_with(os.path.abspath('sample.conf'))

        self.assertEqual(1, execute_mock.call_count)
        execute_mock.assert_called_with()

        self.assertEqual(1, main_mock.call_count)
        config_used = main_mock.call_args[0][0]
        self.assertTrue(isinstance(config_used, simple_db_migrate.config.FileConfig))
        self.assertEqual('mysql', config_used.get('database_engine'))
        self.assertEqual('root', config_used.get('database_user'))
        self.assertEqual('', config_used.get('database_password'))
        self.assertEqual('database', config_used.get('database_name'))
        self.assertEqual('host', config_used.get('database_host'))
        self.assertEqual(1234, config_used.get('database_port'))
        self.assertEqual(False, config_used.get('utc_timestamp'))
        self.assertEqual('__db_version__', config_used.get('database_version_table'))
        self.assertEqual([os.path.abspath('.')], config_used.get("database_migrations_dir"))
        self.assertEqual(None, config_used.get('schema_version'))
        self.assertEqual(False, config_used.get('show_sql'))
        self.assertEqual(False, config_used.get('show_sql_only'))
        self.assertEqual(None, config_used.get('new_migration'))
        self.assertEqual(False, config_used.get('drop_db_first'))
        self.assertEqual(False, config_used.get('paused_mode'))
        self.assertEqual(None, config_used.get('log_dir'))
        self.assertEqual(None, config_used.get('label_version'))
        self.assertEqual(False, config_used.get('force_use_files_on_down'))
        self.assertEqual(False, config_used.get('force_execute_old_migrations_versions'))
        self.assertEqual(1, config_used.get('log_level'))

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    def test_it_should_get_configuration_exclusively_from_args_if_not_use_configuration_file_using_config_class_and_execute_with_default_configuration(self, main_mock, execute_mock):
        simple_db_migrate.run_from_argv(['--db-host', 'host', '--db-port', '4321', '--db-name', 'name', '--db-user', 'user', '--db-password', 'pass', '--db-engine', 'engine', '--db-migrations-dir', '.:/tmp:../migration'])

        self.assertEqual(1, execute_mock.call_count)
        execute_mock.assert_called_with()

        self.assertEqual(1, main_mock.call_count)
        config_used = main_mock.call_args[0][0]
        self.assertTrue(isinstance(config_used, simple_db_migrate.config.Config))
        self.assertEqual('engine', config_used.get('database_engine'))
        self.assertEqual('user', config_used.get('database_user'))
        self.assertEqual('pass', config_used.get('database_password'))
        self.assertEqual('name', config_used.get('database_name'))
        self.assertEqual('host', config_used.get('database_host'))
        self.assertEqual(4321, config_used.get('database_port'))
        self.assertEqual(False, config_used.get('utc_timestamp'))
        self.assertEqual('__db_version__', config_used.get('database_version_table'))
        self.assertEqual([os.path.abspath('.'), '/tmp', os.path.abspath('../migration')], config_used.get("database_migrations_dir"))
        self.assertEqual(None, config_used.get('schema_version'))
        self.assertEqual(False, config_used.get('show_sql'))
        self.assertEqual(False, config_used.get('show_sql_only'))
        self.assertEqual(None, config_used.get('new_migration'))
        self.assertEqual(False, config_used.get('drop_db_first'))
        self.assertEqual(False, config_used.get('paused_mode'))
        self.assertEqual(None, config_used.get('log_dir'))
        self.assertEqual(None, config_used.get('label_version'))
        self.assertEqual(False, config_used.get('force_use_files_on_down'))
        self.assertEqual(False, config_used.get('force_execute_old_migrations_versions'))
        self.assertEqual(1, config_used.get('log_level'))

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    @patch.object(simple_db_migrate.helpers.Utils, 'get_variables_from_file', return_value = {'DATABASE_HOST':'host', 'DATABASE_USER': 'root', 'DATABASE_PASSWORD':'', 'DATABASE_NAME':'database', 'DATABASE_MIGRATIONS_DIR':'.'})
    def test_it_should_use_log_level_as_specified(self, import_file_mock, main_mock, execute_mock):
        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf'), '--log-level', 4])
        config_used = main_mock.call_args[0][0]
        self.assertEqual(4, config_used.get('log_level'))

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    @patch.object(simple_db_migrate.helpers.Utils, 'get_variables_from_file', return_value = {'DATABASE_HOST':'host', 'DATABASE_USER': 'root', 'DATABASE_PASSWORD':'', 'DATABASE_NAME':'database', 'DATABASE_MIGRATIONS_DIR':'.'})
    def test_it_should_use_log_level_as_2_when_in_paused_mode(self, import_file_mock, main_mock, execute_mock):
        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf'), '--pause'])
        config_used = main_mock.call_args[0][0]
        self.assertEqual(2, config_used.get('log_level'))

    @patch('simple_db_migrate.getpass', return_value='password_asked')
    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    @patch.object(simple_db_migrate.helpers.Utils, 'get_variables_from_file', return_value = {'DATABASE_HOST':'host', 'DATABASE_USER': 'root', 'DATABASE_PASSWORD':'<<ask_me>>', 'DATABASE_NAME':'database', 'DATABASE_MIGRATIONS_DIR':'.'})
    def test_it_should_ask_for_password_when_configuration_is_as_ask_me(self, import_file_mock, main_mock, execute_mock, getpass_mock):
        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf')])
        config_used = main_mock.call_args[0][0]
        self.assertEqual('password_asked', config_used.get('database_password'))
        self.assertEqual('\nPlease inform password to connect to database "root@host:database"\n', sys.stdout.getvalue())

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    @patch.object(simple_db_migrate.helpers.Utils, 'get_variables_from_file', return_value = {'DATABASE_HOST':'host', 'DATABASE_USER': 'root', 'DATABASE_PASSWORD':'<<ask_me>>', 'DATABASE_NAME':'database', 'DATABASE_MIGRATIONS_DIR':'.'})
    def test_it_should_use_password_from_command_line_when_configuration_is_as_ask_me(self, import_file_mock, main_mock, execute_mock):
        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf'), '--password', 'xpto_pass'])
        config_used = main_mock.call_args[0][0]
        self.assertEqual('xpto_pass', config_used.get('database_password'))

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    @patch.object(simple_db_migrate.helpers.Utils, 'get_variables_from_file', return_value = {'force_execute_old_migrations_versions':True, 'label_version':'label', 'DATABASE_HOST':'host', 'DATABASE_USER': 'root', 'DATABASE_PASSWORD':'', 'DATABASE_NAME':'database', 'DATABASE_MIGRATIONS_DIR':'.'})
    def test_it_should_use_values_from_config_file_in_replacement_for_command_line(self, import_file_mock, main_mock, execute_mock):
        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf')])
        config_used = main_mock.call_args[0][0]
        self.assertEqual('label', config_used.get('label_version'))
        self.assertEqual(True, config_used.get('force_execute_old_migrations_versions'))

    @patch.object(simple_db_migrate.main.Main, 'execute')
    @patch.object(simple_db_migrate.main.Main, '__init__', return_value=None)
    def test_it_should_check_if_has_a_default_configuration_file(self, main_mock, execute_mock):
        f = open('simple-db-migrate.conf', 'w')
        f.write("DATABASE_HOST = 'host_on_default_configuration_filename'")
        f.close()

        simple_db_migrate.run_from_argv([])
        self.assertEqual(1, main_mock.call_count)
        config_used = main_mock.call_args[0][0]
        self.assertTrue(isinstance(config_used, simple_db_migrate.config.FileConfig))
        self.assertEqual('host_on_default_configuration_filename', config_used.get('database_host'))

        main_mock.reset_mock()

        f = open('sample.conf', 'w')
        f.write("DATABASE_HOST = 'host_on_sample_configuration_filename'")
        f.close()

        simple_db_migrate.run_from_argv(["-c", os.path.abspath('sample.conf')])
        self.assertEqual(1, main_mock.call_count)
        config_used = main_mock.call_args[0][0]
        self.assertTrue(isinstance(config_used, simple_db_migrate.config.FileConfig))
        self.assertEqual('host_on_sample_configuration_filename', config_used.get('database_host'))

    @patch.dict('sys.modules', MySQLdb=MagicMock())
    @patch.object(simple_db_migrate.main.Main, 'labels', return_value=["v1", "foo", "v3"])
    def test_it_should_print_labels_on_database_and_exit(self, labels_mock):
        try:
            simple_db_migrate.run_from_argv(["--info", "labels", "-c", os.path.abspath('sample.conf')])
        except SystemExit as e:
            self.assertEqual(0, e.code)

        self.assertEqual('v1\nfoo\nv3\n\n', sys.stdout.getvalue())

    @patch.dict('sys.modules', MySQLdb=MagicMock())
    @patch.object(simple_db_migrate.main.Main, 'labels', return_value=[])
    def test_it_should_print_none_when_there_are_no_labels_on_database_and_exit(self, labels_mock):
        try:
            simple_db_migrate.run_from_argv(["--info", "labels", "-c", os.path.abspath('sample.conf')])
        except SystemExit as e:
            self.assertEqual(0, e.code)

        self.assertEqual('NONE\n\n', sys.stdout.getvalue())

    @patch.dict('sys.modules', MySQLdb=MagicMock())
    @patch.object(simple_db_migrate.main.Main, 'last_label', return_value="v3")
    def test_it_should_print_last_label_on_database_and_exit(self, last_label_mock):
        try:
            simple_db_migrate.run_from_argv(["--info", "last_label", "-c", os.path.abspath('sample.conf')])
        except SystemExit as e:
            self.assertEqual(0, e.code)

        self.assertEqual('v3\n\n', sys.stdout.getvalue())

    @patch.dict('sys.modules', MySQLdb=MagicMock())
    @patch.object(simple_db_migrate.main.Main, 'last_label', return_value=None)
    def test_it_should_print_none_as_last_label_when_there_are_no_labels_on_database_and_exit(self, last_label_mock):
        try:
            simple_db_migrate.run_from_argv(["--info", "last_label", "-c", os.path.abspath('sample.conf')])
        except SystemExit as e:
            self.assertEqual(0, e.code)

        self.assertEqual('NONE\n\n', sys.stdout.getvalue())

    @patch.dict('sys.modules', MySQLdb=MagicMock())
    def test_it_should_print_error_message_and_exit_when_required_info_is_not_valid(self):
        try:
            simple_db_migrate.run_from_argv(["--info", "not_valid", "-c", os.path.abspath('sample.conf')])
        except SystemExit as e:
            self.assertEqual(1, e.code)

        self.assertEqual("[ERROR] The 'not_valid' is a wrong parameter for info\n\n", sys.stdout.getvalue())


if __name__ == '__main__':
    unittest.main()

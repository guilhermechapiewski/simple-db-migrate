import unittest
from mock import patch
from StringIO import StringIO
from simple_db_migrate.cli import *

class CLITest(unittest.TestCase):

    def setUp(self):
        self.color = CLI.color

    def tearDown(self):
        CLI.color = self.color

    def test_it_should_define_colors_values_as_empty_strings_by_default(self):
        self.assertEqual("", CLI.color["PINK"])
        self.assertEqual("", CLI.color["BLUE"])
        self.assertEqual("", CLI.color["CYAN"])
        self.assertEqual("", CLI.color["GREEN"])
        self.assertEqual("", CLI.color["YELLOW"])
        self.assertEqual("", CLI.color["RED"])
        self.assertEqual("", CLI.color["END"])

    def test_it_should_define_colors_values_when_asked_to_show_collors(self):
        CLI.show_colors()
        self.assertEqual("\033[35m", CLI.color["PINK"])
        self.assertEqual("\033[34m", CLI.color["BLUE"])
        self.assertEqual("\033[36m", CLI.color["CYAN"])
        self.assertEqual("\033[32m", CLI.color["GREEN"])
        self.assertEqual("\033[33m", CLI.color["YELLOW"])
        self.assertEqual("\033[31m", CLI.color["RED"])
        self.assertEqual("\033[0m", CLI.color["END"])

    def test_it_should_exit_with_help_options(self):
        try:
            CLI.parse(["-h"])
        except SystemExit, e:
            self.assertEqual(0, e.code)

        try:
            CLI.parse(["--help"])
        except SystemExit, e:
            self.assertEqual(0, e.code)

    def test_it_should_not_has_a_default_value_for_configuration_file(self):
        self.assertEqual(None, CLI.parse([])[0].config_file)

    def test_it_should_accept_configuration_file_options(self):
        self.assertEqual("file.conf", CLI.parse(["-c", "file.conf"])[0].config_file)
        self.assertEqual("file.conf", CLI.parse(["--config", "file.conf"])[0].config_file)

    def test_it_should_has_a_default_value_for_log_level(self):
        self.assertEqual(1, CLI.parse([])[0].log_level)

    def test_it_should_accept_log_level_options(self):
        self.assertEqual("log_level_value", CLI.parse(["-l", "log_level_value"])[0].log_level)
        self.assertEqual("log_level_value", CLI.parse(["--log-level", "log_level_value"])[0].log_level)

    def test_it_should_not_has_a_default_value_for_log_dir(self):
        self.assertEqual(None, CLI.parse([])[0].log_dir)

    def test_it_should_accept_log_dir_options(self):
        self.assertEqual("log_dir_value", CLI.parse(["--log-dir", "log_dir_value"])[0].log_dir)

    def test_it_should_has_a_default_value_for_force_old_migrations(self):
        self.assertEqual(False, CLI.parse([])[0].force_execute_old_migrations_versions)

    def test_it_should_accept_force_old_migrations_options(self):
        self.assertEqual(True, CLI.parse(["--force-old-migrations"])[0].force_execute_old_migrations_versions)
        self.assertEqual(True, CLI.parse(["--force-execute-old-migrations-versions"])[0].force_execute_old_migrations_versions)

    def test_it_should_has_a_default_value_for_force_files(self):
        self.assertEqual(False, CLI.parse([])[0].force_use_files_on_down)

    def test_it_should_accept_force_files_options(self):
        self.assertEqual(True, CLI.parse(["--force-files"])[0].force_use_files_on_down)
        self.assertEqual(True, CLI.parse(["--force-use-files-on-down"])[0].force_use_files_on_down)

    def test_it_should_not_has_a_default_value_for_schema_version(self):
        self.assertEqual(None, CLI.parse([])[0].schema_version)

    def test_it_should_accept_schema_version_options(self):
        self.assertEqual("schema_version_value", CLI.parse(["-m", "schema_version_value"])[0].schema_version)
        self.assertEqual("schema_version_value", CLI.parse(["--config", "schema_version_value"])[0].config_file)

    def test_it_should_not_has_a_default_value_for_new_migration(self):
        self.assertEqual(None, CLI.parse([])[0].new_migration)

    def test_it_should_accept_new_migration_options(self):
        self.assertEqual("new_migration_value", CLI.parse(["-n", "new_migration_value"])[0].new_migration)
        self.assertEqual("new_migration_value", CLI.parse(["--new", "new_migration_value"])[0].new_migration)
        self.assertEqual("new_migration_value", CLI.parse(["--create", "new_migration_value"])[0].new_migration)

    def test_it_should_has_a_default_value_for_paused_mode(self):
        self.assertEqual(False, CLI.parse([])[0].paused_mode)

    def test_it_should_accept_paused_mode_options(self):
        self.assertEqual(True, CLI.parse(["-p"])[0].paused_mode)
        self.assertEqual(True, CLI.parse(["--paused-mode"])[0].paused_mode)

    def test_it_should_has_a_default_value_for_simple_db_migrate_version(self):
        self.assertEqual(False, CLI.parse([])[0].simple_db_migrate_version)

    def test_it_should_accept_simple_db_migrate_version_options(self):
        self.assertEqual(True, CLI.parse(["-v"])[0].simple_db_migrate_version)
        self.assertEqual(True, CLI.parse(["--version"])[0].simple_db_migrate_version)

    def test_it_should_has_a_default_value_for_show_colors(self):
        self.assertEqual(False, CLI.parse([])[0].show_colors)

    def test_it_should_accept_show_colors_options(self):
        self.assertEqual(True, CLI.parse(["--color"])[0].show_colors)

    def test_it_should_has_a_default_value_for_drop_db_first(self):
        self.assertEqual(False, CLI.parse([])[0].drop_db_first)

    def test_it_should_accept_drop_db_first_options(self):
        self.assertEqual(True, CLI.parse(["--drop"])[0].drop_db_first)
        self.assertEqual(True, CLI.parse(["--drop-database-first"])[0].drop_db_first)

    def test_it_should_has_a_default_value_for_show_sql(self):
        self.assertEqual(False, CLI.parse([])[0].show_sql)

    def test_it_should_accept_show_sql_options(self):
        self.assertEqual(True, CLI.parse(["--show-sql"])[0].show_sql)

    def test_it_should_has_a_default_value_for_show_sql_only(self):
        self.assertEqual(False, CLI.parse([])[0].show_sql_only)

    def test_it_should_accept_show_sql_only_options(self):
        self.assertEqual(True, CLI.parse(["--show-sql-only"])[0].show_sql_only)

    def test_it_should_not_has_a_default_value_for_label_version(self):
        self.assertEqual(None, CLI.parse([])[0].label_version)

    def test_it_should_accept_label_version_options(self):
        self.assertEqual("label_version_value", CLI.parse(["--label", "label_version_value"])[0].label_version)

    def test_it_should_not_has_a_default_value_for_password(self):
        self.assertEqual(None, CLI.parse([])[0].password)

    def test_it_should_accept_password_options(self):
        self.assertEqual("password_value", CLI.parse(["--password", "password_value"])[0].password)

    def test_it_should_has_a_default_value_for_environment(self):
        self.assertEqual("", CLI.parse([])[0].environment)

    def test_it_should_accept_environment_options(self):
        self.assertEqual("environment_value", CLI.parse(["--env", "environment_value"])[0].environment)
        self.assertEqual("environment_value", CLI.parse(["--environment", "environment_value"])[0].environment)

    def test_it_should_has_a_default_value_for_utc_timestamp(self):
        self.assertEqual(False, CLI.parse([])[0].utc_timestamp)

    def test_it_should_accept_utc_timestamp_options(self):
        self.assertEqual(True, CLI.parse(["--utc-timestamp"])[0].utc_timestamp)

    def test_it_should_not_has_a_default_value_for_database_engine(self):
        self.assertEqual(None, CLI.parse([])[0].database_engine)

    def test_it_should_accept_database_engine_options(self):
        self.assertEqual("engine_value", CLI.parse(["--db-engine", "engine_value"])[0].database_engine)

    def test_it_should_not_has_a_default_value_for_database_version_table(self):
        self.assertEqual(None, CLI.parse([])[0].database_version_table)

    def test_it_should_accept_database_version_table_options(self):
        self.assertEqual("version_table_value", CLI.parse(["--db-version-table", "version_table_value"])[0].database_version_table)

    def test_it_should_not_has_a_default_value_for_database_user(self):
        self.assertEqual(None, CLI.parse([])[0].database_user)

    def test_it_should_accept_database_user_options(self):
        self.assertEqual("user_value", CLI.parse(["--db-user", "user_value"])[0].database_user)

    def test_it_should_not_has_a_default_value_for_database_password(self):
        self.assertEqual(None, CLI.parse([])[0].database_password)

    def test_it_should_accept_database_password_options(self):
        self.assertEqual("password_value", CLI.parse(["--db-password", "password_value"])[0].database_password)

    def test_it_should_not_has_a_default_value_for_database_host(self):
        self.assertEqual(None, CLI.parse([])[0].database_host)

    def test_it_should_accept_database_host_options(self):
        self.assertEqual("host_value", CLI.parse(["--db-host", "host_value"])[0].database_host)

    def test_it_should_not_has_a_default_value_for_database_name(self):
        self.assertEqual(None, CLI.parse([])[0].database_name)

    def test_it_should_accept_database_name_options(self):
        self.assertEqual("name_value", CLI.parse(["--db-name", "name_value"])[0].database_name)

    def test_it_should_not_has_a_default_value_for_migrations_dir(self):
        self.assertEqual(None, CLI.parse([])[0].database_migrations_dir)

    def test_it_should_accept_migrations_dir_options(self):
        self.assertEqual(".:../:/tmp", CLI.parse(["--db-migrations-dir", ".:../:/tmp"])[0].database_migrations_dir)

    @patch('sys.stdout', new_callable=StringIO)
    def test_it_should_call_print_statment_with_the_given_message(self, stdout_mock):
        CLI.msg("message to print")
        self.assertEqual("message to print\n", stdout_mock.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_it_should_call_print_statment_with_the_given_message_and_color_codes_when_colors_are_on(self, stdout_mock):
        CLI.show_colors()
        CLI.msg("message to print")
        self.assertEqual("\x1b[36mmessage to print\x1b[0m\n", stdout_mock.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_it_should_use_color_code_to_the_specified_color(self, stdout_mock):
        CLI.show_colors()
        CLI.msg("message to print", "RED")
        self.assertEqual("\x1b[31mmessage to print\x1b[0m\n", stdout_mock.getvalue())

    @patch('simple_db_migrate.cli.CLI.msg')
    def test_it_should_show_error_message_and_exit(self, msg_mock):
        try:
            CLI.error_and_exit("error test message, dont mind about it :)")
            self.fail("it should not get here")
        except:
            pass
        msg_mock.assert_called_with("[ERROR] error test message, dont mind about it :)\n", "RED")

    @patch('simple_db_migrate.cli.CLI.msg')
    def test_it_should_show_info_message_and_exit(self, msg_mock):
        try:
            CLI.info_and_exit("info test message, dont mind about it :)")
            self.fail("it should not get here")
        except:
            pass
        msg_mock.assert_called_with("info test message, dont mind about it :)\n", "BLUE")

if __name__ == "__main__":
    unittest.main()

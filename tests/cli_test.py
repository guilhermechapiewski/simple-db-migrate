import unittest
from mock import patch
from StringIO import StringIO
from simple_db_migrate.cli import *

class CLITest(unittest.TestCase):

    def setUp(self):
        self.color = CLI.color
        self.cli = CLI()

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
            self.cli.parse(["-h"])
        except SystemExit, e:
            self.assertEqual(0, e.code)

        try:
            self.cli.parse(["--help"])
        except SystemExit, e:
            self.assertEqual(0, e.code)

    def test_it_should_has_a_default_value_for_configuration_file(self):
        self.assertEqual("simple-db-migrate.conf", self.cli.parse([])[0].config_file)

    def test_it_should_accept_configuration_file_options(self):
        self.assertEqual("file.conf", self.cli.parse(["-c", "file.conf"])[0].config_file)
        self.assertEqual("file.conf", self.cli.parse(["--config", "file.conf"])[0].config_file)

    def test_it_should_has_a_default_value_for_log_level(self):
        self.assertEqual(1, self.cli.parse([])[0].log_level)

    def test_it_should_accept_log_level_options(self):
        self.assertEqual("log_level_value", self.cli.parse(["-l", "log_level_value"])[0].log_level)
        self.assertEqual("log_level_value", self.cli.parse(["--log-level", "log_level_value"])[0].log_level)

    def test_it_should_not_has_a_default_value_for_log_dir(self):
        self.assertEqual(None, self.cli.parse([])[0].log_dir)

    def test_it_should_accept_log_dir_options(self):
        self.assertEqual("log_dir_value", self.cli.parse(["--log-dir", "log_dir_value"])[0].log_dir)

    def test_it_should_has_a_default_value_for_force_old_migrations(self):
        self.assertEqual(False, self.cli.parse([])[0].force_execute_old_migrations_versions)

    def test_it_should_accept_force_old_migrations_options(self):
        self.assertEqual(True, self.cli.parse(["--force-old-migrations"])[0].force_execute_old_migrations_versions)
        self.assertEqual(True, self.cli.parse(["--force-execute-old-migrations-versions"])[0].force_execute_old_migrations_versions)

    def test_it_should_has_a_default_value_for_force_files(self):
        self.assertEqual(False, self.cli.parse([])[0].force_use_files_on_down)

    def test_it_should_accept_force_files_options(self):
        self.assertEqual(True, self.cli.parse(["--force-files"])[0].force_use_files_on_down)
        self.assertEqual(True, self.cli.parse(["--force-use-files-on-down"])[0].force_use_files_on_down)

    def test_it_should_not_has_a_default_value_for_schema_version(self):
        self.assertEqual(None, self.cli.parse([])[0].schema_version)

    def test_it_should_accept_schema_version_options(self):
        self.assertEqual("schema_version_value", self.cli.parse(["-m", "schema_version_value"])[0].schema_version)
        self.assertEqual("schema_version_value", self.cli.parse(["--config", "schema_version_value"])[0].config_file)

    def test_it_should_not_has_a_default_value_for_new_migration(self):
        self.assertEqual(None, self.cli.parse([])[0].new_migration)

    def test_it_should_accept_new_migration_options(self):
        self.assertEqual("new_migration_value", self.cli.parse(["-n", "new_migration_value"])[0].new_migration)
        self.assertEqual("new_migration_value", self.cli.parse(["--new", "new_migration_value"])[0].new_migration)
        self.assertEqual("new_migration_value", self.cli.parse(["--create", "new_migration_value"])[0].new_migration)

    def test_it_should_has_a_default_value_for_paused_mode(self):
        self.assertEqual(False, self.cli.parse([])[0].paused_mode)

    def test_it_should_accept_paused_mode_options(self):
        self.assertEqual(True, self.cli.parse(["-p"])[0].paused_mode)
        self.assertEqual(True, self.cli.parse(["--paused-mode"])[0].paused_mode)

    def test_it_should_has_a_default_value_for_simple_db_migrate_version(self):
        self.assertEqual(False, self.cli.parse([])[0].simple_db_migrate_version)

    def test_it_should_accept_simple_db_migrate_version_options(self):
        self.assertEqual(True, self.cli.parse(["-v"])[0].simple_db_migrate_version)
        self.assertEqual(True, self.cli.parse(["--version"])[0].simple_db_migrate_version)

    def test_it_should_has_a_default_value_for_show_colors(self):
        self.assertEqual(False, self.cli.parse([])[0].show_colors)

    def test_it_should_accept_show_colors_options(self):
        self.assertEqual(True, self.cli.parse(["--color"])[0].show_colors)

    def test_it_should_has_a_default_value_for_drop_db_first(self):
        self.assertEqual(False, self.cli.parse([])[0].drop_db_first)

    def test_it_should_accept_drop_db_first_options(self):
        self.assertEqual(True, self.cli.parse(["--drop"])[0].drop_db_first)
        self.assertEqual(True, self.cli.parse(["--drop-database-first"])[0].drop_db_first)

    def test_it_should_has_a_default_value_for_show_sql(self):
        self.assertEqual(False, self.cli.parse([])[0].show_sql)

    def test_it_should_accept_show_sql_options(self):
        self.assertEqual(True, self.cli.parse(["--showsql"])[0].show_sql)

    def test_it_should_has_a_default_value_for_show_sql_only(self):
        self.assertEqual(False, self.cli.parse([])[0].show_sql_only)

    def test_it_should_accept_show_sql_only_options(self):
        self.assertEqual(True, self.cli.parse(["--showsqlonly"])[0].show_sql_only)

    def test_it_should_not_has_a_default_value_for_label_version(self):
        self.assertEqual(None, self.cli.parse([])[0].label_version)

    def test_it_should_accept_label_version_options(self):
        self.assertEqual("label_version_value", self.cli.parse(["--label", "label_version_value"])[0].label_version)

    def test_it_should_not_has_a_default_value_for_password(self):
        self.assertEqual(None, self.cli.parse([])[0].password)

    def test_it_should_accept_password_options(self):
        self.assertEqual("password_value", self.cli.parse(["--password", "password_value"])[0].password)

    def test_it_should_has_a_default_value_for_environment(self):
        self.assertEqual("", self.cli.parse([])[0].environment)

    def test_it_should_accept_environment_options(self):
        self.assertEqual("environment_value", self.cli.parse(["--env", "environment_value"])[0].environment)
        self.assertEqual("environment_value", self.cli.parse(["--environment", "environment_value"])[0].environment)

    @patch('sys.stdout', new_callable=StringIO)
    def test_it_should_call_print_statment_with_the_given_message(self, stdout_mock):
        self.cli.msg("message to print")
        self.assertEqual("message to print\n", stdout_mock.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_it_should_call_print_statment_with_the_given_message_and_color_codes_when_colors_are_on(self, stdout_mock):
        CLI.show_colors()
        self.cli.msg("message to print")
        self.assertEqual("\x1b[36mmessage to print\x1b[0m\n", stdout_mock.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_it_should_use_color_code_to_the_specified_color(self, stdout_mock):
        CLI.show_colors()
        self.cli.msg("message to print", "RED")
        self.assertEqual("\x1b[31mmessage to print\x1b[0m\n", stdout_mock.getvalue())

    @patch('simple_db_migrate.cli.CLI.msg')
    def test_it_should_show_error_message_and_exit(self, msg_mock):
        try:
            self.cli.error_and_exit("error test message, dont mind about it :)")
            self.fail("it should not get here")
        except:
            pass
        msg_mock.assert_called_with("[ERROR] error test message, dont mind about it :)\n", "RED")

    @patch('simple_db_migrate.cli.CLI.msg')
    def test_it_should_show_info_message_and_exit(self, msg_mock):
        try:
            self.cli.info_and_exit("info test message, dont mind about it :)")
            self.fail("it should not get here")
        except:
            pass
        msg_mock.assert_called_with("info test message, dont mind about it :)\n", "BLUE")

if __name__ == "__main__":
    unittest.main()

from optparse import OptionParser
import sys

class CLI(object):

    color = {
        "PINK": "",
        "BLUE": "",
        "CYAN": "",
        "GREEN": "",
        "YELLOW": "",
        "RED": "",
        "END": "",
    }

    @staticmethod
    def show_colors():
        CLI.color = {
            "PINK": "\033[35m",
            "BLUE": "\033[34m",
            "CYAN": "\033[36m",
            "GREEN": "\033[32m",
            "YELLOW": "\033[33m",
            "RED": "\033[31m",
            "END": "\033[0m",
        }

    def __init__(self):
        self.__config_parser()

    def __config_parser(self):
        self.__parser = OptionParser()

        self.__parser.add_option("-c", "--config",
                dest="config_file",
                default="simple-db-migrate.conf",
                help="Use a specific config file. If not provided, will search for 'simple-db-migrate.conf' in the current directory.")

        self.__parser.add_option("-l", "--log-level",
                dest="log_level",
                default=1,
                help="Log level: 0-no log; 1-migrations log; 2-statement execution log (default: %default)")

        self.__parser.add_option("--log-dir",
                dest="log_dir",
                default=None,
                help="Directory to save the log files of execution")

        self.__parser.add_option("--force-old-migrations", "--force-execute-old-migrations-versions",
                action="store_true",
                dest="force_execute_old_migrations_versions",
                default=False,
                help="Forces the use of the old migration files even if the destination version is the same as current destination ")

        self.__parser.add_option("--force-files", "--force-use-files-on-down",
                action="store_true",
                dest="force_use_files_on_down",
                default=False,
                help="Forces the use of the migration files instead of using the field sql_down stored on the version table in database downgrade operations ")

        self.__parser.add_option("-m", "--migration",
                dest="schema_version",
                default=None,
                help="Schema version to migrate to. If not provided will migrate to the last version available in the migrations directory.")

        self.__parser.add_option("-n", "--create", "--new",
                dest="new_migration",
                default=None,
                help="Create migration file with the given nickname. The nickname should contain only lowercase characters and underscore '_'. Example: 'create_table_xyz'.")

        self.__parser.add_option("-p", "--paused-mode",
                action="store_true",
                dest="paused_mode",
                default=False,
                help="Execute in 'paused' mode. In this mode you will need to press <enter> key in order to execute each SQL command, making it easier to see what is being executed and helping debug. When paused mode is enabled, log level is automatically set to [2].")

        self.__parser.add_option("-v", "--version",
                action="store_true",
                dest="simple_db_migrate_version",
                default=False,
                help="Displays simple-db-migrate's version and exit.")

        self.__parser.add_option("--color",
                action="store_true",
                dest="show_colors",
                default=False,
                help="Output with beautiful colors.")

        self.__parser.add_option("--drop", "--drop-database-first",
                action="store_true",
                dest="drop_db_first",
                default=False,
                help="Drop database before running migrations to create everything from scratch. Useful when the database schema is corrupted and the migration scripts are not working.")

        self.__parser.add_option("--showsql",
                action="store_true",
                dest="show_sql",
                default=False,
                help="Show all SQL statements executed.")

        self.__parser.add_option("--showsqlonly",
                action="store_true",
                dest="show_sql_only",
                default=False,
                help="Show all SQL statements that would be executed but DON'T execute them in the database.")

        self.__parser.add_option("--label",
                dest="label_version",
                help="Give this label the last migration executed or execute a down to him.")

        self.__parser.add_option("--password",
                dest="password",
                help="Use this password to connect to database, to auto.")

        self.__parser.add_option("--env",
                dest="environment",
                default="",
                help="Use this environment to get specific configurations.")

    def get_parser(self):
        return self.__parser

    def parse(self):
        return self.__parser.parse_args()

    def error_and_exit(self, msg):
        self.msg("[ERROR] %s\n" % msg, "RED")
        sys.exit(1)

    def info_and_exit(self, msg):
        self.msg("%s\n" % msg, "BLUE")
        sys.exit(0)

    def msg(self, msg, color="CYAN"):
        print "%s%s%s" % (self.color[color], msg, self.color["END"])

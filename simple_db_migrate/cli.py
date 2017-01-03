from optparse import OptionParser, make_option
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

    @staticmethod
    def parse(args=None):
        parser = OptionParser()
        for option in CLI.options_to_parser():
          names=option["opt_str"]
          del option["opt_str"]
          parser.add_option(*names, **option)

        parser.add_option("-v", "--version",
                action="store_true",
                dest="simple_db_migrate_version",
                default=False,
                help="Displays simple-db-migrate's version and exit.")
        return parser.parse_args(args)

    @classmethod
    def options_to_parser(cls):
        return (
            {
                "opt_str": ("-c", "--config",),
                "dest": "config_file",
                "default": None,
                "help": "Use a specific config file. If not provided, will search for 'simple-db-migrate.conf' in the current directory."
            },

            {
                "opt_str": ("-l", "--log-level",),
                "dest": "log_level",
                "default": 1,
                "help": "Log level: 0-no log; 1-migrations log; 2-statement execution log (default: 1)"
            },

            {
                "opt_str": ("--log-dir",),
                "dest": "log_dir",
                "default": None,
                "help": "Directory to save the log files of execution"
            },

            {
                "opt_str": ("--force-old-migrations", "--force-execute-old-migrations-versions",),
                "action": "store_true",
                "dest": "force_execute_old_migrations_versions",
                "default": False,
                "help": "Forces the use of the old migration files even if the destination version is the same as current destination "
            },

            {
                "opt_str": ("--force-files", "--force-use-files-on-down",),
                "action": "store_true",
                "dest": "force_use_files_on_down",
                "default": False,
                "help": "Forces the use of the migration files instead of using the field sql_down stored on the version table in database downgrade operations "
            },

            {
                "opt_str": ("-m", "--migration",),
                "dest": "schema_version",
                "default": None,
                "help": "Schema version to migrate to. If not provided will migrate to the last version available in the migrations directory."
            },

            {
                "opt_str": ("-n", "--create", "--new",),
                "dest": "new_migration",
                "default": None,
                "help": "Create migration file with the given nickname. The nickname should contain only lowercase characters and underscore '_'. Example: 'create_table_xyz'."
            },

            {
                "opt_str": ("-p", "--paused-mode",),
                "action": "store_true",
                "dest": "paused_mode",
                "default": False,
                "help": "Execute in 'paused' mode. In this mode you will need to press <enter> key in order to execute each SQL command, making it easier to see what is being executed and helping debug. When paused mode is enabled, log level is automatically set to [2]."
            },

            {
                "opt_str": ("--color",),
                "action": "store_true",
                "dest": "show_colors",
                "default": False,
                "help": "Output with beautiful colors."
            },

            {
                "opt_str": ("--drop", "--drop-database-first",),
                "action": "store_true",
                "dest": "drop_db_first",
                "default": False,
                "help": "Drop database before running migrations to create everything from scratch. Useful when the database schema is corrupted and the migration scripts are not working."
            },

            {
                "opt_str": ("--show-sql",),
                "action": "store_true",
                "dest": "show_sql",
                "default": False,
                "help": "Show all SQL statements executed."
            },

            {
                "opt_str": ("--show-sql-only",),
                "action": "store_true",
                "dest": "show_sql_only",
                "default": False,
                "help": "Show all SQL statements that would be executed but DON'T execute them in the database."
            },

            {
                "opt_str": ("--label",),
                "dest": "label_version",
                "default": None,
                "help": "Give this label the migrations executed or execute a down to him."
            },

            {
                "opt_str": ("--password",),
                "dest": "password",
                "default": None,
                "help": "Use this password to connect to database, to auto."
            },

            {
                "opt_str": ("--env", "--environment",),
                "dest": "environment",
                "default": "",
                "help": "Use this environment to get specific configurations."
            },

            {
                "opt_str": ("--utc-timestamp",),
                "action": "store_true",
                "dest": "utc_timestamp",
                "default": False,
                "help": "Use utc datetime value on the name of migration when creating one."
            },

            {
                "opt_str": ("--db-engine",),
                "dest": "database_engine",
                "default": None,
                "help": "Set each engine to use as sgdb (mysql, oracle, mssql). (default: 'mysql')"
            },

            {
                "opt_str": ("--db-version-table",),
                "dest": "database_version_table",
                "default": None,
                "help": "Set the name of the table used to save migrations history. (default: '__db_version__')"
            },

            {
                "opt_str": ("--db-user",),
                "dest": "database_user",
                "default": None,
                "help": "Set the username to connect to database."
            },

            {
                "opt_str": ("--db-password",),
                "dest": "database_password",
                "default": None,
                "help": "Set the password to connect to database."
            },

            {
                "opt_str": ("--db-host",),
                "dest": "database_host",
                "default": None,
                "help": "Set the host where the database is."
            },

            {
                "opt_str": ("--db-port",),
                "dest": "database_port",
                "default": None,
                "type": int,
                "help": "Set the port where the database is."
            },

            {
                "opt_str": ("--db-name",),
                "dest": "database_name",
                "default": None,
                "help": "Set the name of the database."
            },

            {
                "opt_str": ("--db-migrations-dir",),
                "dest": "database_migrations_dir",
                "default": None,
                "help": "List of directories where migrations are separated by a colon"
            },

            {
                "opt_str": ("--info",),
                "dest": "info_database",
                "default": None,
                "help": "Show info of applied migrations (options: labels, last_label)"
            },
        )

    @classmethod
    def error_and_exit(cls, msg):
        cls.msg("[ERROR] %s\n" % msg, "RED")
        sys.exit(1)

    @classmethod
    def info_and_exit(cls, msg):
        cls.msg("%s\n" % msg, "BLUE")
        sys.exit(0)

    @classmethod
    def msg(cls, msg, color="CYAN"):
        print("{}{}{}".format(cls.color[color], msg, cls.color["END"]))

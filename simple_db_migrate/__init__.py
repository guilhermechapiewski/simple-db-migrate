from getpass import getpass
import codecs
import sys

from cli import CLI
from config import FileConfig, Config
from main import Main

SIMPLE_DB_MIGRATE_VERSION = '1.5.0'

# fixing print in non-utf8 terminals
if sys.stdout.encoding != 'UTF-8':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

def run_from_argv(args=None):
    (options, args) = CLI.parse(args)
    run(options.__dict__)

def run(options):
    try:
        if options.get('simple_db_migrate_version'):
            msg = 'simple-db-migrate v%s' % SIMPLE_DB_MIGRATE_VERSION
            CLI.info_and_exit(msg)

        if options.get('show_colors'):
            CLI.show_colors()

        # Create config
        if options.get('config_file'):
            config = FileConfig(options.get('config_file'), options.get('environment'))
        else:
            config = Config()

        config.update('schema_version', options.get('schema_version'))
        config.update('show_sql', options.get('show_sql'))
        config.update('show_sql_only', options.get('show_sql_only'))
        config.update('new_migration', options.get('new_migration'))
        config.update('drop_db_first', options.get('drop_db_first'))
        config.update('paused_mode', options.get('paused_mode'))
        config.update('log_dir', options.get('log_dir'))
        config.update('label_version', options.get('label_version'))
        config.update('force_use_files_on_down', options.get('force_use_files_on_down'))
        config.update('force_execute_old_migrations_versions', options.get('force_execute_old_migrations_versions'))
        config.update('utc_timestamp', options.get('utc_timestamp'))
        config.update('database_user', options.get('database_user'))
        config.update('database_password', options.get('database_password'))
        config.update('database_host', options.get('database_host'))
        config.update('database_name', options.get('database_name'))
        if options.get('database_migrations_dir'):
            config.update("database_migrations_dir", Config._parse_migrations_dir(options.get('database_migrations_dir')))

        config.update('database_engine', options.get('database_engine'))
        if not config.get('database_engine', None):
            config.update('database_engine', "mysql")

        config.update('database_version_table', options.get('database_version_table'))
        if not config.get('database_version_table', None):
            config.update('database_version_table', "__db_version__")

        # paused mode forces log_level to 2
        log_level = int(options.get('log_level'))
        if options.get('paused_mode'):
            log_level = 2

        config.update('log_level', log_level)

        # Ask the password for user if configured
        if config.get('database_password') == '<<ask_me>>':
            if options.get('password'):
                passwd = options.get('password')
            else:
                CLI.msg('\nPlease inform password to connect to database "%s@%s:%s"' % (config.get('database_user'), config.get('database_host'), config.get('database_name')))
                passwd = getpass()
            config.update('database_password', passwd)

        # If CLI was correctly parsed, execute db-migrate.
        Main(config).execute()
    except KeyboardInterrupt:
        CLI.info_and_exit("\nExecution interrupted by user...")
    except Exception, e:
        CLI.error_and_exit(str(e))

if __name__ == '__main__':
    run_from_argv()

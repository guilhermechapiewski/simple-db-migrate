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

def run(args=None):
    cli = CLI()
    try:
        (options, args) = cli.parse(args)

        if options.simple_db_migrate_version:
            msg = 'simple-db-migrate v%s' % SIMPLE_DB_MIGRATE_VERSION
            CLI.info_and_exit(msg)

        if options.show_colors:
            CLI.show_colors()

        # Create config
        if options.config_file:
            config = FileConfig(options.config_file, options.environment)
        else:
            config = Config()

        config.update('schema_version', options.schema_version)
        config.update('show_sql', options.show_sql)
        config.update('show_sql_only', options.show_sql_only)
        config.update('new_migration', options.new_migration)
        config.update('drop_db_first', options.drop_db_first)
        config.update('paused_mode', options.paused_mode)
        config.update('log_dir', options.log_dir)
        config.update('label_version', options.label_version)
        config.update('force_use_files_on_down', options.force_use_files_on_down)
        config.update('force_execute_old_migrations_versions', options.force_execute_old_migrations_versions)
        config.update('utc_timestamp', options.utc_timestamp)
        config.update('database_engine', options.database_engine)
        config.update('database_version_table', options.database_version_table)
        config.update('database_user', options.database_user)
        config.update('database_password', options.database_password)
        config.update('database_host', options.database_host)
        config.update('database_name', options.database_name)
        if options.database_migrations_dir:
            config.update("database_migrations_dir", Config._parse_migrations_dir(options.database_migrations_dir))

        # paused mode forces log_level to 2
        log_level = int(options.log_level)
        if options.paused_mode:
            log_level = 2

        config.update('log_level', log_level)

        # Ask the password for user if configured
        if config.get('database_password') == '<<ask_me>>':
            if options.password:
                passwd = options.password
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
    run()

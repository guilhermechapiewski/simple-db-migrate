from getpass import getpass
import codecs
import sys

from cli import CLI
from config import FileConfig
from main import Main

SIMPLE_DB_MIGRATE_VERSION = '1.4.4'

# fixing print in non-utf8 terminals
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

def run(args=None, getpass=getpass):
    cli = CLI()
    try:
        (options, args) = cli.parse(args)

        if options.simple_db_migrate_version:
            msg = 'simple-db-migrate v%s' % SIMPLE_DB_MIGRATE_VERSION
            cli.info_and_exit(msg)

        if options.show_colors:
            CLI.show_colors()

        # Create config
        config = FileConfig(options.config_file, options.environment)
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

        # paused mode forces log_level to 2
        log_level = int(options.log_level)
        if options.paused_mode:
            log_level = 2

        config.update('log_level', log_level)

        # Ask the password for user if configured
        if config.get('db_password') == '<<ask_me>>':
            if options.password:
                passwd = options.password
            else:
                cli.msg('\nPlease inform password to connect to database "%s@%s:%s"' % (config.get('db_user'), config.get('db_host'), config.get('db_name')))
                passwd = getpass()
            config.update('db_password', passwd)

        # If CLI was correctly parsed, execute db-migrate.
        Main(config).execute()
    except KeyboardInterrupt:
        cli.info_and_exit("\nExecution interrupted by user...")
    except Exception, e:
        cli.error_and_exit(str(e))

if __name__ == '__main__':
    run()

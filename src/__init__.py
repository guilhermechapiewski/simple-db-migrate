from getpass import getpass
import codecs
import sys

from cli import CLI
from config import FileConfig
from main import Main

SIMPLE_DB_MIGRATE_VERSION = '1.3.8'

# fixing print in non-utf8 terminals
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

def run():
    cli = CLI()
    try:
        (options, args) = cli.parse()

        if options.simple_db_migrate_version:
            msg = 'simple-db-migrate v%s' % SIMPLE_DB_MIGRATE_VERSION
            cli.info_and_exit(msg)

        if options.show_colors:
            CLI.show_colors()

        # Create config
        config = FileConfig(options.config_file)
        config.put('schema_version', options.schema_version)
        config.put('show_sql', options.show_sql)
        config.put('show_sql_only', options.show_sql_only)
        config.put('new_migration', options.new_migration)
        config.put('drop_db_first', options.drop_db_first)
        config.put('paused_mode', options.paused_mode)
        
        # paused mode forces log_level to 2
        log_level = int(options.log_level)
        if options.paused_mode:
            log_level = 2
        
        config.put('log_level', log_level)
        
        # Ask the password for user if configured
        if config.get('db_password') == '<<ask_me>>':
            cli.msg('\nPlease inform password to connect to database "%s@%s:%s"' % (config.get('db_user'), config.get('db_host'), config.get('db_name')))
            passwd = getpass()
            config.remove('db_password')
            config.put('db_password', passwd)

        # If CLI was correctly parsed, execute db-migrate.
        Main(config).execute()
    except KeyboardInterrupt:
        cli.info_and_exit("\nExecution interrupted by user...")
    except Exception, e:
        cli.error_and_exit(str(e))
        
if __name__ == '__main__':
    run()

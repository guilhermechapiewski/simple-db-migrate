from cli import CLI
from core import SimpleDBMigrate
from helpers import Lists
from mysql import MySQL
import sys

class Main(object):
    
    def __init__(self, options=None, args=None, mysql=None, db_migrate=None):
        self.__cli = CLI()
        self.__options = options
        self.__args = args
        
        self.__mysql = mysql
        if self.__mysql is None and not self.__options.create_migration:
            self.__mysql = MySQL(db_config_file=self.__options.db_config_file, drop_db_first=self.__options.drop_db_first)
        
        self.__db_migrate = db_migrate
        if self.__db_migrate is None:
            self.__db_migrate = SimpleDBMigrate(self.__options.migrations_dir)
    
    def execute(self):
        self.__cli.msg("\nStarting DB migration...")
        if self.__options.create_migration:
            self._create_migration()
        else:
            self._migrate()
        self.__cli.msg("\nDone.\n")
            
    def _create_migration(self):
        new_file = self.__db_migrate.create_migration(self.__options.create_migration)
        self.__cli.msg("- Created file '%s'" % (new_file))
    
    def _migrate(self):
        destination_version = self._get_destination_version()
        current_version = self.__mysql.get_current_schema_version()
        
        self.__cli.msg("- Current version is: %s" % current_version)
        self.__cli.msg("- Destination version is: %s" % destination_version)

        # if current and destination versions are the same, 
        # will consider a migration up to execute remaining files
        is_migration_up = True
        if int(current_version) > int(destination_version):
            is_migration_up = False

        # do it!
        self._execute_migrations(current_version, destination_version, is_migration_up)

    def _get_destination_version(self):
        destination_version = self.__options.schema_version
        if destination_version is None:
            destination_version = self.__db_migrate.latest_schema_version_available()

        if not self.__db_migrate.check_if_version_exists(destination_version):
            self.__cli.error_and_exit("version not found (%s)" % destination_version)

        return destination_version
        
    def _get_migration_files_to_be_executed(self, current_version, destination_version):
        mysql_versions = self.__mysql.get_all_schema_versions()
        migration_versions = self.__db_migrate.get_all_migration_versions()
        
        # migration up: the easy part
        if current_version <= destination_version:
            remaining_versions_to_execute = Lists.subtract(migration_versions, mysql_versions)
            remaining_versions_to_execute = [version for version in remaining_versions_to_execute if version <= destination_version]
            return remaining_versions_to_execute
        
        # migration down...
        down_versions = [version for version in mysql_versions if version <= current_version and version > destination_version]
        for version in down_versions:
            if version not in migration_versions:
                self.__cli.error_and_exit("impossible to migrate down: one of the versions was not found (%s)" % version)
        down_versions.reverse()
        return down_versions
        
    def _execute_migrations(self, current_version, destination_version, is_migration_up):
        # getting only the migration sql files to be executed
        versions_to_be_executed = self._get_migration_files_to_be_executed(current_version, destination_version)
        
        if versions_to_be_executed is None or len(versions_to_be_executed) == 0:
            self.__cli.msg("\nNothing to do.\n")
            sys.exit(0)
        
        up_down_label = "up" if is_migration_up else "down"
        self.__cli.msg("\nStarting migration %s!" % up_down_label)
        self.__cli.msg("*** will run %s\n" % versions_to_be_executed)
        
        sql_statements_executed = ""
        for migration_version in versions_to_be_executed:
            sql_file = self.__db_migrate.get_migration_file_name_from_version_number(migration_version)

            self.__cli.msg("===== executing %s (%s) =====" % (sql_file, up_down_label))
            sql = self.__db_migrate.get_sql_command(sql_file, is_migration_up)
            self.__mysql.change(sql, migration_version, is_migration_up)
            
            #recording the last statement executed
            sql_statements_executed += sql
        
        if self.__options.show_sql:
            self.__cli.msg("__________ SQL statements executed __________")
            self.__cli.msg(sql_statements_executed)
            self.__cli.msg("_____________________________________________\n")
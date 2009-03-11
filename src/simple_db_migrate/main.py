from core import SimpleDBMigrate
from mysql import MySQL
from logging import Log

class Main(object):
    def __init__(self, options=None, args=None):
        self.__options = options
        self.__args = args
        self.__mysql = MySQL(self.__options.db_config_file)
        self.__db_migrate = SimpleDBMigrate(self.__options.migrations_dir)
    
    def execute(self):
        print "\nStarting DB migration..."
        if self.__options.create_migration:
            self.__create_migration()
        else:
            self.__migrate()
        print "\nDone.\n"
            
    def __create_migration(self):
        new_file = self.__db_migrate.create_migration(self.__options.create_migration)
        print "- Created file '%s'" % (new_file)
    
    def __migrate(self):
        destination_version = self.__get_destination_version()
        current_version = self.__mysql.get_current_schema_version()
        
        if str(current_version) == str(destination_version):
            Log().error_and_exit("current and destination versions are the same (%s)" % current_version)

        print "- Current version is: %s" % current_version
        print "- Destination version is: %s" % destination_version

        # if current and destination versions are the same, 
        # will consider a migration up to execute remaining files
        is_migration_up = True
        if int(current_version) > int(destination_version):
            is_migration_up = False

        print "\nStarting migration %s!\n" % "up" if is_migration_up else "down"

        # do it!
        self.__execute_migrations(current_version, destination_version, is_migration_up)

    def __get_destination_version(self):
        destination_version = self.__options.schema_version

        if destination_version == None:
            destination_version = self.__db_migrate.latest_schema_version_available()

        if not self.__db_migrate.check_if_version_exists(destination_version):
            Log().error_and_exit("version not found (%s)" % destination_version)

        return destination_version
                
    def __execute_migrations(self, current_version, destination_version, is_migration_up):
        # getting only the migration sql files to be executed
        migration_files_to_be_executed = self.__db_migrate.get_migration_files_between_versions(current_version, destination_version)
        
        sql_statements_executed = ""
        for sql_file in migration_files_to_be_executed:    

            file_version = self.__db_migrate.get_migration_version(sql_file)
            if not is_migration_up:
                file_version = destination_version
            
            print "===== executing %s (%s) =====" % (sql_file, "up" if is_migration_up else "down")
            sql = self.__db_migrate.get_sql_command(sql_file, is_migration_up)
            self.__mysql.change(sql, file_version, is_migration_up)
            
            #recording the last statement executed
            sql_statements_executed += sql
        
        if self.__options.show_sql:
            print "__________ SQL statements executed __________"
            print sql_statements_executed
            print "_____________________________________________\n"
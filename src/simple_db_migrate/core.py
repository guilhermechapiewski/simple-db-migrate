from logging import *
from mysql import MySQL
import os
import sys

class Main(object):

    def execute(self, options, args):
    
        print "\nStarting DB migration..."

        mysql = MySQL(options.db_config_file)
        db_migrate = SimpleDBMigrate(options.migrations_dir)

        destination_version = options.schema_version

        if destination_version == None:
            destination_version = db_migrate.latest_schema_version_available()

        if not db_migrate.check_if_version_exists(destination_version):
            Log().error_and_exit("version not found (%s)" % destination_version)

        current_version = mysql.get_current_schema_version()
        if str(current_version) == str(destination_version):
            Log().error_and_exit("current and destination versions are the same (%s)" % current_version)

        print "- Current version is: %s" % current_version
        print "- Destination version is: %s" % destination_version

        migration_up = True
        migration = "up"
        if int(current_version) > int(destination_version):
            migration_up = False
            migration = "down"

        print "\nStarting migration %s!\n" % migration

        # getting only the migration sql files to be executed
        migration_files_to_be_executed = db_migrate.get_migration_files_between_versions(current_version, destination_version)

        for sql_file in migration_files_to_be_executed:    

            file_version = db_migrate.get_migration_version(sql_file)
            if not migration_up:
                file_version = destination_version
            
            print "===== executing %s (%s) =====" % (sql_file, migration)
            sql = db_migrate.get_sql_command(sql_file, migration_up)
            mysql.change(sql, file_version)
        
        print "\nDone.\n"

class SimpleDBMigrate(object):
    
    __migration_files_extension = ".migration"
    
    def __init__(self, migrations_dir):
        self.__migrations_dir = migrations_dir

    def get_all_migration_files(self):
        dir_list = os.listdir(self.__migrations_dir)
        
        files = []
        for dir_file in dir_list:
            if dir_file.endswith(self.__migration_files_extension):
                files.append(dir_file)
        
        if len(files) == 0:
            Log().error_and_exit("no migration files found")
        
        files.sort()
        
        return files
        
    def get_sql_command(self, sql_file, migration_up=True):
        f = open(self.__migrations_dir + "/" + sql_file, "r")
        exec(f.read())
        f.close()
        
        if migration_up:
            return SQL_UP
        else:
            return SQL_DOWN
    
    def get_migration_version(self, sql_file):
        return sql_file[0:sql_file.find("_")]
        
    def check_if_version_exists(self, version):
        files = self.get_all_migration_files()
        for f in files:
            if f.startswith(version):
                return True
        return False       
    
    def get_migration_files_between_versions(self, current_version, destination_version):
        #TODO: make it less idiot :)
        migration_up = True
        if int(current_version) > int(destination_version):
            migration_up = False
        
        all_files = self.get_all_migration_files()
        if not migration_up:
            all_files.reverse()
            
        migration_files = []
        for f in all_files:
            f_version = self.get_migration_version(f)
            if migration_up:
                if int(f_version) > int(current_version) and int(f_version) <= int(destination_version):
                    migration_files.append(f)
            else:
                if int(f_version) > int(destination_version) and int(f_version) <= int(current_version):
                    migration_files.append(f)
            
        return migration_files
        
    def latest_schema_version_available(self):
        all_files = self.get_all_migration_files()
        
        all_files.sort()
        all_files.reverse()
        
        return self.get_migration_version(all_files[0])

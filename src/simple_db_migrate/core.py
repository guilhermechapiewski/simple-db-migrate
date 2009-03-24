from cli import CLI
from time import strftime
import os
import shutil
import re

#TODO: Refactor: rename to "Migrations"
class SimpleDBMigrate(object):
    
    __migration_files_extension = ".migration"
    
    def __init__(self, migrations_dir):
        self.__migrations_dir = migrations_dir
        self.__cli = CLI()

    def get_all_migration_files(self):
        path = os.path.abspath(self.__migrations_dir)
        dir_list = None
        try:
            dir_list = os.listdir(path)
        except OSError:
            self.__cli.error_and_exit("directory not found ('%s')" % path)
        
        files = []
        for dir_file in dir_list:
            if self.is_file_name_valid(dir_file):
                files.append(dir_file)
        
        if len(files) == 0:
            self.__cli.error_and_exit("no migration files found")
        
        files.sort()
        
        return files
        
    def get_sql_command(self, sql_file, migration_up=True):
        try:
            f = open(self.__migrations_dir + "/" + sql_file, "r")
            exec(f.read())
        except IOError:
            self.__cli.error_and_exit("%s: file not found" % self.__migrations_dir + "/" + sql_file)
        else:
            f.close()
        
        try:
            (SQL_UP, SQL_DOWN)
        except NameError:
            self.__cli.error_and_exit("migration file is incorrect; it does not define 'SQL_UP' or 'SQL_DOWN' ('%s')" % sql_file)
        
        sql = ""
        sql = SQL_UP if migration_up else SQL_DOWN
        
        if sql is None or sql == "":
            self.__cli.error_and_exit("migration command is empty ('%s')" % sql_file)
        
        return sql    
    
    def get_all_migration_versions(self):
        versions = []
        migration_files = self.get_all_migration_files()
        for each_file in migration_files:
            versions.append(self.get_migration_version(each_file))
        return versions
    
    def get_all_migration_versions_up_to(self, limit_version):
        all_versions = self.get_all_migration_versions()
        return [version for version in all_versions if version < limit_version]
    
    def get_migration_version(self, sql_file):
        return sql_file[0:sql_file.find("_")]
        
    def check_if_version_exists(self, version):
        files = self.get_all_migration_files()
        for f in files:
            if f.startswith(version):
                return True
        return False
        
    def latest_schema_version_available(self):
        all_files = self.get_all_migration_files()
        
        all_files.sort()
        all_files.reverse()
        
        return self.get_migration_version(all_files[0])
    
    def is_file_name_valid(self, file_name):
        mask = r"[0-9]{14}\w+%s$" % self.__migration_files_extension
        match = re.match(mask, file_name, re.IGNORECASE)
        return match != None
        
    def create_migration(self, migration_name):
        timestamp = strftime("%Y%m%d%H%M%S")        
        file_name = "%s_%s%s" % (timestamp, migration_name, self.__migration_files_extension)
        
        if not self.is_file_name_valid(file_name):
            self.__cli.error_and_exit("invalid migration name; it should contain only letters, numbers and/or underscores ('%s')" % migration_name)
        
        new_file = "%s/%s" % (self.__migrations_dir, file_name)
        
        try:
            f = open(new_file, "w")
            f.write(MigrationFile.template)
            f.close()
        except IOError:
            self.__cli.error_and_exit("could not create file ('%s')" % new_file)
            
        return file_name
    
    def get_migration_file_name_from_version_number(self, version):
        all_files = self.get_all_migration_files()
        for f in all_files:
            if f.startswith(version):
                return f
        return None
        
class MigrationFile(object):
    template = '''SQL_UP = """

"""

SQL_DOWN = """

"""'''
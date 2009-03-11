from logging import *
from mysql import MySQL
from time import strftime
import os
import shutil
import re

class SimpleDBMigrate(object):
    __migration_files_extension = ".migration"
    
    def __init__(self, migrations_dir):
        self.__migrations_dir = migrations_dir

    def get_all_migration_files(self):
        dir_list = os.listdir(self.__migrations_dir)
        
        files = []
        for dir_file in dir_list:
            if self.is_file_name_valid(dir_file):
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
    
    def get_all_migration_versions(self):
        versions = []
        migration_files = self.get_all_migration_files()
        for each_file in migration_files:
            versions.append(self.get_migration_version(each_file))
        return versions
    
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
    
    def is_file_name_valid(self, file_name):
        mask = r"[0-9]{14}\w+%s" % self.__migration_files_extension
        match = re.match(mask, file_name, re.IGNORECASE)
        return match != None
        
    def create_migration(self, migration_name):
        timestamp = strftime("%Y%m%d%H%M%S")        
        file_name = "%s_%s%s" % (timestamp, migration_name, self.__migration_files_extension)
        
        if not self.is_file_name_valid(file_name):
            Log().error_and_exit("invalid migration name; it should contain only letters, numbers and/or underscores ('%s')" % migration_name)
        
        new_file = "%s/%s" % (self.__migrations_dir, file_name)
        
        try:
            f = open(new_file, "w")
            f.write(MigrationFile.template)
            f.close()
        except IOError:
            Log().error_and_exit("could not create file ('%s')" % new_file)
            
        return file_name
        
class MigrationFile(object):
    template = '''SQL_UP = """

"""

SQL_DOWN = """
"""'''
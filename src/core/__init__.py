from time import strftime
import codecs
import os
import shutil
import re

class Migration(object):
    
    MIGRATION_FILES_EXTENSION = ".migration"
    MIGRATION_FILES_MASK = r"[0-9]{14}\w+%s$" % MIGRATION_FILES_EXTENSION
    TEMPLATE = 'SQL_UP = u"""\n\n"""\n\nSQL_DOWN = u"""\n\n"""'
    
    def __init__(self, file):
        file_name = os.path.split(file)[1]
        if not Migration.is_file_name_valid(file_name):
            raise Exception('invalid migration file name (%s)' % file_name)
        
        if not os.path.exists(file):
            raise Exception('migration file does not exist (%s)' % file)
    
        self.abspath = os.path.abspath(file)
        self.file_name = file_name
        self.version = file_name[0:file_name.find("_")]
        self.sql_up, self.sql_down = self._get_commands()
    
    def _get_commands(self):
        f = codecs.open(self.abspath, "rU", "utf-8")
        exec(f.read())
        f.close()

        try:
            (SQL_UP, SQL_DOWN)
        except NameError:
            raise Exception("migration file is incorrect; it does not define 'SQL_UP' or 'SQL_DOWN' (%s)" % self.abspath)

        if SQL_UP is None or SQL_UP == "":
            raise Exception("migration command 'SQL_UP' is empty (%s)" % self.abspath)

        if SQL_DOWN is None or SQL_DOWN == "":
            raise Exception("migration command 'SQL_DOWN' is empty (%s)" % self.abspath)

        return SQL_UP, SQL_DOWN
    
    def compare_to(self, another_migration):
        if self.version < another_migration.version:
            return -1
        elif self.version > another_migration.version:
            return 1
        return 0
    
    @staticmethod
    def sort_migrations_list(migrations, reverse=False):
        return sorted(migrations, cmp=lambda x,y: x.compare_to(y), reverse=reverse)
    
    @staticmethod
    def is_file_name_valid(file_name):
        match = re.match(Migration.MIGRATION_FILES_MASK, file_name, re.IGNORECASE)
        return match != None

    @staticmethod
    def create(migration_name, migration_dir='.'):
        timestamp = strftime("%Y%m%d%H%M%S")        
        file_name = "%s_%s%s" % (timestamp, migration_name, Migration.MIGRATION_FILES_EXTENSION)

        if not Migration.is_file_name_valid(file_name):
            raise Exception("invalid migration name ('%s'); it should contain only letters, numbers and/or underscores" % migration_name)

        new_file_name = "%s/%s" % (migration_dir, file_name)

        try:
            f = codecs.open(new_file_name, "w", "utf-8")
            f.write(Migration.TEMPLATE)
            f.close()
        except IOError:
            raise Exception("could not create file ('%s')" % new_file_name)

        return new_file_name
        
class SimpleDBMigrate(object):
    
    def __init__(self, config=None):
        self._migrations_dir = config.get("migrations_dir")
    
    def get_all_migrations(self):
        migrations = []
        
        for dir in self._migrations_dir:
            path = os.path.abspath(dir)
            
            dir_list = None
            try:
                dir_list = os.listdir(path)
            except OSError:
                raise Exception("directory not found ('%s')" % path)
        
            for dir_file in dir_list:
                if dir_file.endswith(Migration.MIGRATION_FILES_EXTENSION) and Migration.is_file_name_valid(dir_file):
                    migration = Migration('%s/%s' % (path, dir_file))
                    migrations.append(migration)
        
        if len(migrations) == 0:
            raise Exception("no migration files found")
        
        return Migration.sort_migrations_list(migrations)
            
    def get_all_migration_versions(self):
        return [migration.version for migration in self.get_all_migrations()]
    
    def get_all_migration_versions_up_to(self, limit_version):
        return [version for version in self.get_all_migration_versions() if version < limit_version]
    
    def check_if_version_exists(self, version):
        return version in self.get_all_migration_versions()
        
    def latest_version_available(self):
        all_migrations = self.get_all_migrations()
        all_migrations = Migration.sort_migrations_list(all_migrations, reverse=True)
        return all_migrations[0].version
    
    def get_migration_from_version_number(self, version):
        migrations = [migration for migration in self.get_all_migrations() if migration.version == version]
        if len(migrations) > 0:
            return migrations[0]
        return None

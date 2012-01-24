from time import strftime, gmtime, localtime
import codecs
import os
import shutil
import re
import imp
import tempfile
import sys

class Migration(object):

    MIGRATION_FILES_EXTENSION = ".migration"
    MIGRATION_FILES_MASK = r"[0-9]{14}\w+%s$" % MIGRATION_FILES_EXTENSION
    TEMPLATE = '#-*- coding:%s -*-\nSQL_UP = u"""\n\n"""\n\nSQL_DOWN = u"""\n\n"""\n'

    def __init__(self, file=None, id=0, file_name="", version="", label=None, sql_up="", sql_down="", script_encoding="utf-8"):
        self.id = id
        self.file_name = file_name
        self.version = version
        self.sql_up = sql_up
        self.sql_down = sql_down
        self.script_encoding = script_encoding
        self.label = label
        self.abspath = ""
        if file:
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
        SQL_UP = ''
        SQL_DOWN = ''
        mod = None
        temp_abspath = None

        try:
            mod = imp.load_source(self.file_name, self.abspath)
            SQL_UP = Migration.check_sql_unicode(mod.SQL_UP, self.script_encoding)
            SQL_DOWN = Migration.check_sql_unicode(mod.SQL_DOWN, self.script_encoding)
        except Exception:
            try:
                f = open(self.abspath, "rU")
                content = f.read()
                f.close()

                temp_abspath = "%s/%s" %(tempfile.gettempdir().rstrip('/'), self.file_name)
                f = open(temp_abspath, "w")
                f.write('#-*- coding:%s -*-\n%s' % (self.script_encoding, content))
                f.close()

                mod = imp.load_source(self.file_name, temp_abspath)

                SQL_UP = Migration.check_sql_unicode(mod.SQL_UP, self.script_encoding)
                SQL_DOWN = Migration.check_sql_unicode(mod.SQL_DOWN, self.script_encoding)

            except Exception:
                f = codecs.open(self.abspath, "rU", self.script_encoding)
                exec(f.read())
                f.close()
        finally:
            #erase temp and compiled files
            if temp_abspath and os.path.isfile(temp_abspath):
                os.remove(temp_abspath)

            if mod and sys.modules.has_key(self.file_name):
                sys.modules.pop(self.file_name)

            if temp_abspath and os.path.isfile(temp_abspath + "c"):
                os.remove(temp_abspath + "c")

            if os.path.isfile(self.abspath + "c"):
                os.remove(self.abspath + "c")

        try:
            (SQL_UP, SQL_DOWN)
        except NameError:
            raise Exception("migration file is incorrect; it does not define 'SQL_UP' or 'SQL_DOWN' (%s)" % self.abspath)

        if SQL_UP is None or SQL_UP == "":
            raise Exception("migration command 'SQL_UP' is empty (%s)" % self.abspath)

        if SQL_DOWN is None or SQL_DOWN == "":
            raise Exception("migration command 'SQL_DOWN' is empty (%s)" % self.abspath)

        return SQL_UP, SQL_DOWN

    @staticmethod
    def check_sql_unicode(sql, script_encoding):
        if not sql or not script_encoding:
            return ""

        try:
            sql = unicode(sql.decode(script_encoding))
        except UnicodeEncodeError:
            sql = unicode(sql)
        return sql

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
    def create(migration_name, migration_dir='.', script_encoding='utf-8', utc_timestamp = False):
        timestamp = strftime("%Y%m%d%H%M%S", gmtime() if utc_timestamp else localtime())
        file_name = "%s_%s%s" % (timestamp, migration_name, Migration.MIGRATION_FILES_EXTENSION)

        if not Migration.is_file_name_valid(file_name):
            raise Exception("invalid migration name ('%s'); it should contain only letters, numbers and/or underscores" % migration_name)

        new_file_name = "%s/%s" % (migration_dir, file_name)

        try:
            f = codecs.open(new_file_name, "w", script_encoding)
            f.write(Migration.TEMPLATE % (script_encoding))
            f.close()
        except IOError:
            raise Exception("could not create file ('%s')" % new_file_name)

        return new_file_name

class SimpleDBMigrate(object):

    def __init__(self, config=None):
        self._migrations_dir = config.get("migrations_dir")
        self._script_encoding=config.get("db_script_encoding", "utf-8")
        self.all_migrations = self.get_all_migrations()

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
                    migration = Migration('%s/%s' % (path, dir_file), script_encoding=self._script_encoding)
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
        migrations = [migration for migration in self.all_migrations if migration.version == version]
        if len(migrations) > 0:
            return migrations[0]
        return None

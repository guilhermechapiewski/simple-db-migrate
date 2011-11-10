import os
import sys
import re
import ast

class Config(object):

    DB_VERSION_TABLE = "__db_version__"

    def __init__(self):
        self._config = {}

    def __repr__(self):
        return str(self._config)

    #default_value was assigned as !@#$%&* to be more easy to check when the default value is None, empty string or False
    def get(self, config_key, default_value='!@#$%&*'):
        try:
            return self._config[config_key]
        except KeyError:
            if default_value != '!@#$%&*':
                return default_value
            raise Exception("invalid configuration key ('%s')" % config_key)

    def put(self, config_key, config_value):
        if config_key in self._config:
            raise Exception("the configuration key '%s' already exists and you cannot override any configuration" % config_key)
        self._config[config_key] = config_value

    def remove(self, config_key):
        try:
            del self._config[config_key]
        except KeyError:
            raise Exception("invalid configuration key ('%s')" % config_key)

    def _parse_migrations_dir(self, dirs, config_dir=''):
        abs_dirs = []
        for dir in dirs.split(':'):
            if os.path.isabs(dir):
                abs_dirs.append(dir)
            elif config_dir == '':
                abs_dirs.append(os.path.abspath(dir))
            else:
                abs_dirs.append(os.path.abspath('%s/%s' % (config_dir, dir)))
        return abs_dirs

class FileConfig(Config):

    class SettingsFile(object):
        @staticmethod
        def import_file(full_filename):
            path, filename = os.path.split(full_filename)
            name, extension = os.path.splitext(filename)

            try:
                # add settings dir from path
                sys.path.insert(0, path)

                if extension == '.py':
                    # if is Python, execute as a module
                    exec "from %s import *" % name
                else:
                    # if not, exec the file contents
                    execfile(full_filename)
            except IOError:
                raise Exception("%s: file not found" % full_filename)
            except Exception, e:
                raise Exception("error interpreting config file '%s': %s" % (filename, str(e)))
            finally:
                # remove settings dir from path
                sys.path.remove(path)

            return locals()

    def __init__(self, config_file="simple-db-migrate.conf", environment=''):
        self._config = {}

        # read configuration
        settings = FileConfig.SettingsFile.import_file(config_file)

        self.put("db_host", self.get_variable(settings, 'DATABASE_HOST', 'HOST', environment=environment))
        self.put("db_user", self.get_variable(settings, 'DATABASE_USER', 'USERNAME', environment=environment))
        self.put("db_password", self.get_variable(settings, 'DATABASE_PASSWORD', 'PASSWORD', environment=environment))
        self.put("db_name", self.get_variable(settings, 'DATABASE_NAME', 'DATABASE', environment=environment))

        self.put("db_engine", self.get_variable(settings, 'DATABASE_ENGINE', 'ENGINE', 'mysql', environment))
        self.put("db_version_table", self.get_variable(settings, 'DATABASE_VERSION_TABLE', 'VERSION_TABLE', self.DB_VERSION_TABLE, environment))

        self.put("utc_timestamp", ast.literal_eval(str(self.get_variable(settings, 'UTC_TIMESTAMP', None, 'False', environment))))

        self.get_custom_variables(settings, ['DATABASE_HOST', 'DATABASE_USER', 'DATABASE_PASSWORD', 'DATABASE_NAME', 'DATABASE_ENGINE', 'DATABASE_VERSION_TABLE', 'DATABASE_MIGRATIONS_DIR'])

        migrations_dir = self.get_variable(settings, 'DATABASE_MIGRATIONS_DIR', 'MIGRATIONS_DIR', environment=environment)
        config_dir = os.path.split(config_file)[0]
        self.put("migrations_dir", self._parse_migrations_dir(migrations_dir, config_dir))

    def get_custom_variables(self, settings, default_variables = []):
        """
        Put on config var all custom variables present in config file wich starts with DATABASE_
        """
        p = re.compile("^DATABASE_(?P<name>.*)")
        for key in settings.keys():
            m_key = p.match(key)
            if m_key and key not in default_variables:
                self.put("db_%s" % m_key.group('name').lower(), self.get_variable(settings, key,  m_key.group('name'), environment=environment))

    #default_value was assigned as !@#$%&* to be more easy to check when the default value is None, empty string or False
    def get_variable(self, settings, name, old_name, default_value='!@#$%&*', environment=''):
        if environment:
            environment = environment.upper()
            env_name = environment + "_" + name if name else None
            env_old_name = environment + "_" + old_name if old_name else None
            if env_name in settings or env_old_name in settings:
                return settings.get(env_name, settings.get(env_old_name))

        if name in settings or old_name in settings:
            return settings.get(name, settings.get(old_name))
        else:
            if default_value != '!@#$%&*':
                return default_value
            raise NameError("config file error: name '%s' is not defined" % name)

class InPlaceConfig(Config):

    def __init__(self, db_host, db_user, db_password, db_name, migrations_dir, db_version_table='', log_dir='', db_engine='mysql', utc_timestamp=False):
        if not db_version_table or db_version_table == '':
            db_version_table = self.DB_VERSION_TABLE
        self._config = {
            "db_host": db_host,
            "db_user": db_user,
            "db_password": db_password,
            "db_name": db_name,
            "db_version_table": db_version_table,
            "migrations_dir": self._parse_migrations_dir(migrations_dir),
            "log_dir": log_dir,
            "db_engine" : db_engine,
            "utc_timestamp" : utc_timestamp
        }

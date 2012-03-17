import os
import sys
import re
import ast
from helpers import Utils

class Config(object):

    DB_VERSION_TABLE = "__db_version__"

    def __init__(self, inital_config=None):
        self._config = inital_config or {}

    def __repr__(self):
        return str(self._config)

    #default_value was assigned as !@#$%&* to be more easy to check when the default value is None, empty string or False
    def get(self, config_key, default_value='!@#$%&*'):
        return Config._get(self._config, config_key, default_value)

    def put(self, config_key, config_value):
        if config_key in self._config:
            raise Exception("the configuration key '%s' already exists and you cannot override any configuration" % config_key)
        self._config[config_key] = config_value

    def update(self, config_key, config_value):
        if config_key in self._config:
            value = self.get(config_key)
            self.remove(config_key)
            config_value = config_value or value
        self.put(config_key, config_value)

    def remove(self, config_key):
        try:
            del self._config[config_key]
        except KeyError:
            raise Exception("invalid configuration key ('%s')" % config_key)

    #default_value was assigned as !@#$%&* to be more easy to check when the default value is None, empty string or False
    @staticmethod
    def _get(dict, key, default_value='!@#$%&*'):
        try:
            return dict[key]
        except KeyError:
            if default_value != '!@#$%&*':
                return default_value
            raise Exception("invalid key ('%s')" % key)

    @staticmethod
    def _parse_migrations_dir(dirs, config_dir=''):
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

    def __init__(self, config_file="simple-db-migrate.conf", environment=''):
        self._config = {}

        # read configuration
        settings = Utils.get_variables_from_file(config_file)

        self.put("database_host", FileConfig._get_variable(settings, 'DATABASE_HOST', environment=environment))
        self.put("database_user", FileConfig._get_variable(settings, 'DATABASE_USER', environment=environment))
        self.put("database_password", FileConfig._get_variable(settings, 'DATABASE_PASSWORD', environment=environment))
        self.put("database_name", FileConfig._get_variable(settings, 'DATABASE_NAME', environment=environment))

        self.put("database_engine", FileConfig._get_variable(settings, 'DATABASE_ENGINE', 'mysql', environment))
        self.put("database_version_table", FileConfig._get_variable(settings, 'DATABASE_VERSION_TABLE', self.DB_VERSION_TABLE, environment))

        self.put("utc_timestamp", ast.literal_eval(str(FileConfig._get_variable(settings, 'UTC_TIMESTAMP', 'False', environment))))

        self._get_custom_variables(settings, ['DATABASE_HOST', 'DATABASE_USER', 'DATABASE_PASSWORD', 'DATABASE_NAME', 'DATABASE_ENGINE', 'DATABASE_VERSION_TABLE', 'DATABASE_MIGRATIONS_DIR'], environment=environment)
        self._get_general_variables(settings, environment=environment)

        migrations_dir = FileConfig._get_variable(settings, 'DATABASE_MIGRATIONS_DIR', environment=environment)
        config_dir = os.path.split(config_file)[0]
        self.put("database_migrations_dir", FileConfig._parse_migrations_dir(migrations_dir, config_dir))

    def _get_custom_variables(self, settings, default_variables = [], environment=''):
        """
        Put on config var all custom variables present in config file wich starts with DATABASE_
        """
        p = re.compile("^DATABASE_(?P<name>.*)")
        for key in settings.keys():
            m_key = p.match(key)
            if m_key and key not in default_variables:
                self.put("db_%s" % m_key.group('name').lower(), FileConfig._get_variable(settings, key,  m_key.group('name'), environment=environment))

    def _get_general_variables(self, settings, environment=''):
        """
        Put on config var all variables present in config file wich not starts with DATABASE_
        """
        p = re.compile("^(DATABASE_.*|os|MIGRATIONS_DIR|migrations_dir|UTC_TIMESTAMP|utc_timestamp)$")
        for key in settings.keys():
            m_key = p.match(key)
            if not m_key:
                self.put(key.lower(), FileConfig._get_variable(settings, key, None, environment=environment))

    #default_value was assigned as !@#$%&* to be more easy to check when the default value is None, empty string or False
    @staticmethod
    def _get_variable(settings, name, default_value='!@#$%&*', environment=''):
        if environment:
            try:
                return FileConfig._get_variable(settings, environment.upper() + "_" + name if name else None)
            except Exception, e:
                pass

        try:
            return FileConfig._get(settings, name)
        except Exception, e:
            if (default_value != '!@#$%&*'):
                return default_value

            if environment:
                raise Exception("invalid keys ('%s_%s', '%s')" % (environment.upper(), name, name))

            raise Exception("invalid key ('%s')" % (name))

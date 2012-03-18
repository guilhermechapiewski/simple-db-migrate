import os
import sys
import re
import ast
from helpers import Utils

class Config(object):

    def __init__(self, inital_config=None):
        self._config = inital_config or {}
        for key in self._config.keys():
            self._config[key.lower()] = self._config[key]

    def __repr__(self):
        return str(self._config)

    #default_value was assigned as !@#$%&* to be more easy to check when the default value is None, empty string or False
    def get(self, config_key, default_value='!@#$%&*'):
        config_key = config_key.lower()
        return Config._get(self._config, config_key, default_value)

    def put(self, config_key, config_value):
        config_key = config_key.lower()
        if config_key in self._config:
            raise Exception("the configuration key '%s' already exists and you cannot override any configuration" % config_key)
        self._config[config_key] = config_value

    def update(self, config_key, config_value):
        config_key = config_key.lower()
        if config_key in self._config:
            value = self.get(config_key)
            self.remove(config_key)
            config_value = config_value or value
        self.put(config_key, config_value)

    def remove(self, config_key):
        try:
            config_key = config_key.lower()
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
        # read configuration
        settings = Utils.get_variables_from_file(config_file)

        super(FileConfig, self).__init__(inital_config=settings)

        if environment:
            prefix = environment + "_"
            for key in self._config.keys():
                if key.startswith(prefix):
                    self.update(key[len(prefix):], self.get(key))

        self.update("utc_timestamp", ast.literal_eval(str(self.get("utc_timestamp", 'False'))))

        migrations_dir = self.get("database_migrations_dir", None)
        if migrations_dir:
            config_dir = os.path.split(config_file)[0]
            self.update("database_migrations_dir", FileConfig._parse_migrations_dir(migrations_dir, config_dir))

import codecs
import os

class Config(object):
    
    DB_VERSION_TABLE = "__db_version__"
    
    def __init__(self):
        self._config = {}
        
    def __repr__(self):
        return str(self._config)

    def get(self, config_key):
        try:
            return self._config[config_key]
        except KeyError:
            raise Exception("invalid configuration key ('%s')" % config_key)
            
    def put(self, config_key, config_value):
        if config_key in self._config:
            raise Exception("the configuration key '%s' already exists and you cannot override any configuration" % config_key)
        self._config[config_key] = config_value
        
    def _parse_migrations_dir(self, dirs, config_dir=''):
        abs_dirs = []
        for dir in dirs.split(':'):
            if config_dir == '':
                abs_dirs.append(os.path.abspath(dir))
            else:
                abs_dirs.append(os.path.abspath('%s/%s' % (config_dir, dir)))
        return abs_dirs
    
class FileConfig(Config):
    
    def __init__(self, config_file="simple-db-migrate.conf"):
        self._config = {}
        
        # read configurations
        try:
            f = codecs.open(config_file, "rU", "utf-8")
            exec(f.read())
        except IOError:
            raise Exception("%s: file not found" % config_file)
        else:
            f.close()
        
        try:
            self.put("db_host", HOST)
            self.put("db_user", USERNAME)
            self.put("db_password", PASSWORD)
            self.put("db_name", DATABASE)
            self.put("db_version_table", self.DB_VERSION_TABLE)
            
            config_path = os.path.split(config_file)[0]
            self.put("migrations_dir", self._parse_migrations_dir(MIGRATIONS_DIR, config_path))
        except NameError, e:
            raise Exception("config file error: " + str(e))

class InPlaceConfig(Config):

    def __init__(self, db_host, db_user, db_password, db_name, migrations_dir, db_version_table=''):
        if not db_version_table or db_version_table == '':
            db_version_table = self.DB_VERSION_TABLE
        self._config = {
            "db_host": db_host,
            "db_user": db_user,
            "db_password": db_password,
            "db_name": db_name,
            "db_version_table": db_version_table,
            "migrations_dir": self._parse_migrations_dir(migrations_dir)
        }
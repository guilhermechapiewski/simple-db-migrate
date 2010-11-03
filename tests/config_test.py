import codecs
import unittest

from simple_db_migrate.config import *

class ConfigTest(unittest.TestCase):
    
    def test_it_should_parse_migrations_dir_with_one_relative_dir(self):
        config = Config()
        dirs = config._parse_migrations_dir('.')
        assert len(dirs) == 1
        assert dirs[0] == os.path.abspath('.')
    
    def test_it_should_parse_migrations_dir_with_two_relative_dirs(self):
        config = Config()
        dirs = config._parse_migrations_dir('test:migrations:./a/relative/path:another/path')
        assert len(dirs) == 4
        assert dirs[0] == os.path.abspath('test')
        assert dirs[1] == os.path.abspath('migrations')
        assert dirs[2] == os.path.abspath('./a/relative/path')
        assert dirs[3] == os.path.abspath('another/path')
        
    def test_it_should_parse_migrations_dir_with_one_absolute_dir(self):
        config = Config()
        dirs = config._parse_migrations_dir(os.path.abspath('.'))
        assert len(dirs) == 1
        assert dirs[0] == os.path.abspath('.')

    def test_it_should_parse_migrations_dir_with_two_absolute_dirs(self):
        config = Config()
        dirs = config._parse_migrations_dir('%s:%s:%s:%s' % (
                os.path.abspath('test'), os.path.abspath('migrations'), 
                os.path.abspath('./a/relative/path'), os.path.abspath('another/path'))
        )
        assert len(dirs) == 4
        assert dirs[0] == os.path.abspath('test')
        assert dirs[1] == os.path.abspath('migrations')
        assert dirs[2] == os.path.abspath('./a/relative/path')
        assert dirs[3] == os.path.abspath('another/path')

class FileConfigTest(unittest.TestCase):
    
    def setUp(self):
        config_file = '''
HOST = os.getenv('DB_HOST') or 'localhost'
USERNAME = os.getenv('DB_USERNAME') or 'root'
PASSWORD = os.getenv('DB_PASSWORD') or ''
DATABASE = os.getenv('DB_DATABASE') or 'migration_example'
MIGRATIONS_DIR = os.getenv('MIGRATIONS_DIR') or 'example'
'''
        f = open('sample.conf', 'w')
        f.write(config_file)
        f.close()
        
    def tearDown(self):
        os.remove('sample.conf')
    
    def test_it_should_read_config_file(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        self.assertEquals(config.get('db_host'), 'localhost')
        self.assertEquals(config.get('db_user'), 'root')
        self.assertEquals(config.get('db_password'), '')
        self.assertEquals(config.get('db_name'), 'migration_example')
        self.assertEquals(config.get('db_version_table'), Config.DB_VERSION_TABLE)
        self.assertEquals(config.get('migrations_dir'), [os.path.abspath('example')])

    def test_it_should_stop_execution_when_an_invalid_key_is_requested(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        try:
            config.get('invalid_config')
            self.fail('it should not pass here')
        except:
            pass
    
    def test_it_should_create_new_configs(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        
        # ensure that the config does not exist
        self.assertRaises(Exception, config.get, 'sample_config')
        
        # create the config
        config.put('sample_config', 'TEST')
        
        # read the config
        self.assertEquals(config.get('sample_config'), 'TEST')
        
    def test_it_should_not_override_existing_configs(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        config.put('sample_config', 'TEST')
        self.assertRaises(Exception, config.put, 'sample_config', 'TEST')
        
    def test_it_should_get_local_variable_values(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        
        weird_variable = 'weird_value'
        result = config.get_variable(locals(), 'weird_variable', 'weirdest_variable')
        
        assert result == weird_variable
        
    def test_it_should_delete_config(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        config.put('sample_config', 'TEST')
        
        assert config.get('sample_config') == 'TEST'
        
        config.remove('sample_config')
        
        self.assertRaises(Exception, config.get, 'sample_config')

class InPlaceConfigTest(unittest.TestCase):
    
    def test_it_should_configure_default_parameters(self):
        config = InPlaceConfig('localhost', 'user', 'passwd', 'db', 'dir')
        self.assertEquals(config.get('db_host'), 'localhost') 
        self.assertEquals(config.get('db_user'), 'user')
        self.assertEquals(config.get('db_password'), 'passwd')
        self.assertEquals(config.get('db_name'), 'db')
        self.assertEquals(config.get('db_version_table'), Config.DB_VERSION_TABLE)
        self.assertEquals(config.get('migrations_dir'), [os.path.abspath('dir')])
        self.assertEquals(config.get('log_dir'), '')
    
    def test_it_should_stop_execution_when_an_invalid_key_is_requested(self):
        config = InPlaceConfig('localhost', 'user', 'passwd', 'db', 'dir')
        try:
            config.get('invalid_config')
            self.fail('it should not pass here')
        except:
            pass
    
    def test_it_should_create_new_configs(self):
        config = InPlaceConfig('localhost', 'user', 'passwd', 'db', 'dir')
        
        # ensure that the config does not exist
        self.assertRaises(Exception, config.get, 'sample_config')
        
        # create the config
        config.put('sample_config', 'TEST')
        
        # read the config
        self.assertEquals(config.get('sample_config'), 'TEST')
        
    def test_it_should_not_override_existing_configs(self):
        config = InPlaceConfig('localhost', 'user', 'passwd', 'db', 'dir')
        config.put('sample_config', 'TEST')
        self.assertRaises(Exception, config.put, 'sample_config', 'TEST')
        

if __name__ == '__main__':
    unittest.main()

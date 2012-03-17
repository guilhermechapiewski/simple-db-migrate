import codecs
import unittest
from mock import patch, MagicMock, NonCallableMagicMock
from simple_db_migrate.config import *

class ConfigTest(unittest.TestCase):

    def test_it_should_parse_migrations_dir_with_one_relative_dir(self):
        dirs = Config._parse_migrations_dir('.')
        self.assertEqual(1, len(dirs))
        self.assertEqual(os.path.abspath('.'), dirs[0])

    def test_it_should_parse_migrations_dir_with_multiple_relative_dirs(self):
        dirs = Config._parse_migrations_dir('test:migrations:./a/relative/path:another/path')
        self.assertEqual(4, len(dirs))
        self.assertEqual(os.path.abspath('test'), dirs[0])
        self.assertEqual(os.path.abspath('migrations'), dirs[1])
        self.assertEqual(os.path.abspath('./a/relative/path'), dirs[2])
        self.assertEqual(os.path.abspath('another/path'), dirs[3])

    def test_it_should_parse_migrations_dir_with_one_absolute_dir(self):
        dirs = Config._parse_migrations_dir(os.path.abspath('.'))
        self.assertEqual(1, len(dirs))
        self.assertEqual(os.path.abspath('.'), dirs[0])

    def test_it_should_parse_migrations_dir_with_multiple_absolute_dirs(self):
        dirs = Config._parse_migrations_dir('%s:%s:%s:%s' % (
                os.path.abspath('test'), os.path.abspath('migrations'),
                os.path.abspath('./a/relative/path'), os.path.abspath('another/path'))
        )
        self.assertEqual(4, len(dirs))
        self.assertEqual(os.path.abspath('test'), dirs[0])
        self.assertEqual(os.path.abspath('migrations'), dirs[1])
        self.assertEqual(os.path.abspath('./a/relative/path'), dirs[2])
        self.assertEqual(os.path.abspath('another/path'), dirs[3])

    def test_it_should_parse_migrations_dir_with_mixed_relative_and_absolute_dirs(self):
        dirs = Config._parse_migrations_dir('%s:%s:%s:%s' % ('/tmp/test', '.', './a/relative/path', os.path.abspath('another/path')))
        self.assertEqual(4, len(dirs))
        self.assertEqual('/tmp/test', dirs[0])
        self.assertEqual(os.path.abspath('.'), dirs[1])
        self.assertEqual(os.path.abspath('./a/relative/path'), dirs[2])
        self.assertEqual(os.path.abspath('another/path'), dirs[3])

    def test_it_should_parse_migrations_dir_with_relative_dirs_using_config_dir_parameter_as_base_path(self):
        dirs = Config._parse_migrations_dir(
                '%s:%s:%s:%s' % ('/tmp/test', '.', './a/relative/path', os.path.abspath('another/path')),
                config_dir='/base/path_to_relative_dirs'
        )
        self.assertEqual(4, len(dirs))
        self.assertEqual('/tmp/test', dirs[0])
        self.assertEqual('/base/path_to_relative_dirs', dirs[1])
        self.assertEqual('/base/path_to_relative_dirs/a/relative/path', dirs[2])
        self.assertEqual(os.path.abspath('another/path'), dirs[3])


    def test_it_should_return_value_from_a_dict(self):
        dict = {"some_key": "some_value"}
        self.assertEqual("some_value", Config._get(dict, "some_key"))

    def test_it_should_return_default_value_for_an_inexistent_dict_value(self):
        dict = {"some_key": "some_value"}
        self.assertEqual("default_value", Config._get(dict, "ANOTHER_KEY", "default_value"))

    def test_it_should_raise_exception_for_an_inexistent_config_value_without_specify_a_default_value(self):
        dict = {"some_key": "some_value"}
        try:
            Config._get(dict, "ANOTHER_KEY")
        except Exception, e:
            self.assertEqual("invalid key ('ANOTHER_KEY')", str(e))

    def test_it_should_accept_non_empty_stringand_false_as_default_value(self):
        dict = {"some_key": "some_value"}
        self.assertEqual(None, Config._get(dict,"ANOTHER_KEY", None))
        self.assertEqual("", Config._get(dict,"ANOTHER_KEY", ""))
        self.assertEqual(False, Config._get(dict,"ANOTHER_KEY", False))

    def test_it_should_save_config_values(self):
        config = Config()
        initial = str(config)
        config.put("some_key", "some_value")
        self.assertNotEqual(initial, str(config))

    def test_it_should_not_update_saved_config_values(self):
        config = Config()
        config.put("some_key", "some_value")
        try:
            config.put("some_key", "another_value")
        except Exception, e:
            self.assertEqual("the configuration key 'some_key' already exists and you cannot override any configuration", str(e))

    def test_it_should_remove_saved_config_values(self):
        config = Config()
        config.put("some_key", "some_value")
        initial = str(config)
        config.remove("some_key")
        self.assertNotEqual(initial, str(config))

    def test_it_should_raise_exception_when_removing_an_inexistent_config_value(self):
        config = Config()
        config.put("some_key", "some_value")
        try:
            config.remove("ANOTHER_KEY")
        except Exception, e:
            self.assertEqual("invalid configuration key ('ANOTHER_KEY')", str(e))

    def test_it_should_return_previous_saved_config_values(self):
        config = Config()
        config.put("some_key", "some_value")
        self.assertEqual("some_value", config.get("some_key"))

    def test_it_should_accept_initial_values_as_configuration(self):
        config = Config({"some_key": "some_value"})
        self.assertEqual("some_value", config.get("some_key"))

    def test_it_should_return_default_value_for_an_inexistent_config_value(self):
        config = Config()
        config.put("some_key", "some_value")
        self.assertEqual("default_value", config.get("ANOTHER_KEY", "default_value"))

    def test_it_should_raise_exception_for_an_inexistent_config_value_without_specify_a_default_value(self):
        config = Config()
        config.put("some_key", "some_value")
        try:
            config.get("ANOTHER_KEY")
        except Exception, e:
            self.assertEqual("invalid key ('ANOTHER_KEY')", str(e))

    def test_it_should_accept_non_empty_string_and_false_as_default_value(self):
        config = Config()
        config.put("some_key", "some_value")
        self.assertEqual(None, config.get("ANOTHER_KEY", None))
        self.assertEqual("", config.get("ANOTHER_KEY", ""))
        self.assertEqual(False, config.get("ANOTHER_KEY", False))

    def test_it_should_update_value_to_a_non_existing_key(self):
        config = Config()
        config.update("some_key", "some_value")
        self.assertEqual("some_value", config.get("some_key"))

    def test_it_should_update_value_to_a_existing_key(self):
        config = Config()
        config.put("some_key", "original_value")
        config.update("some_key", "some_value")
        self.assertEqual("some_value", config.get("some_key"))

    def test_it_should_update_value_to_a_existing_key_keeping_original_value_if_new_value_is_none_false_or_empty_string(self):
        config = Config()
        config.put("some_key", "original_value")
        config.update("some_key", None)
        self.assertEqual("original_value", config.get("some_key"))
        config.update("some_key", False)
        self.assertEqual("original_value", config.get("some_key"))
        config.update("some_key", "")
        self.assertEqual("original_value", config.get("some_key"))

class FileConfigTest(unittest.TestCase):

    def setUp(self):
        config_file = '''
DATABASE_HOST = 'localhost'
DATABASE_USER = 'root'
DATABASE_PASSWORD = ''
DATABASE_NAME = 'migration_example'
ENV1_DATABASE_NAME = 'migration_example_env1'
DATABASE_MIGRATIONS_DIR = 'example'
UTC_TIMESTAMP = True
DATABASE_ANY_CUSTOM_VARIABLE = 'Some Value'
SOME_ENV_DATABASE_ANY_CUSTOM_VARIABLE = 'Other Value'
DATABASE_OTHER_CUSTOM_VARIABLE = 'Value'
'''
        f = open('sample.conf', 'w')
        f.write(config_file)
        f.close()

        f = open('sample.py', 'w')
        f.write('import os\n')
        f.write(config_file)
        f.close()

    def tearDown(self):
        os.remove('sample.conf')
        os.remove('sample.py')

    def test_it_should_return_value_from_a_dict_using_the_given_name(self):
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value"}, "SOME_KEY"))

    def test_it_should_return_default_value_for_an_inexistent_dict_value_using_the_given_name(self):
        dict = {"SOME_KEY": "some_value"}
        self.assertEqual("default_value", FileConfig._get_variable(dict, "ANOTHER_KEY", "default_value"))

    def test_it_should_raise_exception_for_an_inexistent_config_value_without_specify_a_default_value_using_the_given_name(self):
        dict = {"SOME_KEY": "some_value"}
        try:
            FileConfig._get_variable(dict, "ANOTHER_KEY", "OLD_ANOTHER_KEY")
        except Exception, e:
            self.assertEqual("invalid keys ('ANOTHER_KEY', 'OLD_ANOTHER_KEY')", str(e))

    def test_it_should_raise_exception_for_an_inexistent_config_value_without_specify_a_default_value_and_with_environment_using_the_given_name(self):
        dict = {"SOME_KEY": "some_value"}
        try:
            FileConfig._get_variable(dict, "ANOTHER_KEY", environment='some_env')
        except Exception, e:
            self.assertEqual("invalid keys ('SOME_ENV_ANOTHER_KEY', 'ANOTHER_KEY')", str(e))

    def test_it_should_accept_non_empty_string_and_false_as_default_value_using_the_given_name(self):
        dict = {"SOME_KEY": "some_value"}
        self.assertEqual(None, FileConfig._get_variable(dict, "ANOTHER_KEY", None))
        self.assertEqual("", FileConfig._get_variable(dict, "ANOTHER_KEY", ""))
        self.assertEqual(False, FileConfig._get_variable(dict, "ANOTHER_KEY", False))

    def test_it_should_stop_execution_when_a_none_key_is_requested(self):
        dict = {"SOME_KEY": "some_value", "SOME_ENV_SOME_KEY": "other_value"}

        try:
            FileConfig._get_variable(dict, None)
            self.fail('it should not pass here')
        except Exception, e:
            self.assertEqual("invalid key ('None')", str(e))

    def test_it_should_use_environment_as_a_prefix_for_the_given_name(self):
        self.assertEqual("other_value", FileConfig._get_variable({"SOME_KEY": "some_value", "SOME_ENV_SOME_KEY": "other_value"}, "SOME_KEY", environment='some_env'))
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value"}, "SOME_KEY", environment='some_env'))

    def test_it_should_return_non_prefixed_key_when_environment_is_not_available_for_the_given_name(self):
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value", "SOME_ENV_SOME_KEY": "other_value"}, "SOME_KEY", environment='other_env'))
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value"}, "SOME_KEY", environment='other_env'))

    def test_it_should_use_environment_as_a_prefix_for_the_given_name_and_default_value(self):
        self.assertEqual("other_value", FileConfig._get_variable({"SOME_KEY": "some_value", "SOME_ENV_SOME_KEY": "other_value"}, "SOME_KEY", environment='some_env', default_value='value'))
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value"}, "SOME_KEY", environment='some_env', default_value='value'))

    def test_it_should_return_non_prefixed_key_when_environment_is_not_available_for_the_given_names_and_default_value(self):
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value", "SOME_ENV_SOME_KEY": "other_value"}, "SOME_KEY", environment='other_env', default_value='value'))
        self.assertEqual("some_value", FileConfig._get_variable({"SOME_KEY": "some_value"}, "SOME_KEY", environment='other_env', default_value='value'))

    def test_it_should_extend_from_config_class(self):
        config = FileConfig(os.path.abspath('sample.conf'))
        self.assertTrue(isinstance(config, Config))

    def test_it_should_read_config_file(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        self.assertEquals(config.get('database_host'), 'localhost')
        self.assertEquals(config.get('database_user'), 'root')
        self.assertEquals(config.get('database_password'), '')
        self.assertEquals(config.get('database_name'), 'migration_example')
        self.assertEquals(config.get('database_version_table'), Config.DB_VERSION_TABLE)
        self.assertEquals(config.get("database_migrations_dir"), [os.path.abspath('example')])
        self.assertEquals(config.get('utc_timestamp'), True)

    def test_it_should_use_configuration_by_environment(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path, "env1")
        self.assertEquals(config.get('database_name'), 'migration_example_env1')
        self.assertEquals(config.get('database_user'), 'root')

    def test_it_should_stop_execution_when_an_invalid_key_is_requested(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        try:
            config.get('invalid_config')
            self.fail('it should not pass here')
        except Exception, e:
            self.assertEqual("invalid key ('invalid_config')", str(e))

    def test_it_should_get_any_database_custom_variable(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path)
        self.assertEqual('Some Value', config.get('db_any_custom_variable'))

    def test_it_should_get_any_database_custom_variable_using_environment(self):
        config_path = os.path.abspath('sample.conf')
        config = FileConfig(config_path, 'some_env')
        self.assertEqual('Other Value', config.get('db_any_custom_variable'))
        self.assertEqual('Value', config.get('db_other_custom_variable'))

if __name__ == '__main__':
    unittest.main()

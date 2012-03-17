import unittest

from simple_db_migrate.helpers import *

class ListsTest(unittest.TestCase):

    def test_it_should_subtract_lists(self):
        a = ["a", "b", "c", "d", "e", "f"]
        b = ["a", "b", "c", "e"]
        result = Lists.subtract(a, b)

        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], "d")
        self.assertEquals(result[1], "f")

    def test_it_should_subtract_lists2(self):
        a = ["a", "b", "c", "e"]
        b = ["a", "b", "c", "d", "e", "f"]

        result = Lists.subtract(a, b)

        self.assertEquals(len(result), 0)

class UtilsTest(unittest.TestCase):

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

    def test_it_should_count_chars_in_a_string(self):
        word = 'abbbcd;;;;;;;;;;;;;;'
        count = Utils.count_occurrences(word)
        self.assertEqual( 1, count.get('a', 0))
        self.assertEqual( 3, count.get('b', 0))
        self.assertEqual(14, count.get(';', 0))
        self.assertEqual( 0, count.get('%', 0))

    def test_it_should_extract_variables_from_a_config_file(self):
        variables = Utils.get_variables_from_file(os.path.abspath('sample.conf'))
        self.assertEqual('root', variables['DATABASE_USER'])
        self.assertEqual('migration_example_env1', variables['ENV1_DATABASE_NAME'])
        self.assertEqual('migration_example', variables['DATABASE_NAME'])
        self.assertEqual('example', variables['DATABASE_MIGRATIONS_DIR'])
        self.assertEqual(True, variables['UTC_TIMESTAMP'])
        self.assertEqual('localhost', variables['DATABASE_HOST'])
        self.assertEqual('', variables['DATABASE_PASSWORD'])

    def test_it_should_extract_variables_from_a_config_file_with_py_extension(self):
        variables = Utils.get_variables_from_file(os.path.abspath('sample.py'))
        self.assertEqual('root', variables['DATABASE_USER'])
        self.assertEqual('migration_example_env1', variables['ENV1_DATABASE_NAME'])
        self.assertEqual('migration_example', variables['DATABASE_NAME'])
        self.assertEqual('example', variables['DATABASE_MIGRATIONS_DIR'])
        self.assertEqual(True, variables['UTC_TIMESTAMP'])
        self.assertEqual('localhost', variables['DATABASE_HOST'])
        self.assertEqual('', variables['DATABASE_PASSWORD'])

    def test_it_should_not_change_python_path(self):
        original_paths = []
        for path in sys.path:
            original_paths.append(path)

        Utils.get_variables_from_file(os.path.abspath('sample.py'))

        self.assertEqual(original_paths, sys.path)


    def test_it_should_raise_exception_config_file_has_a_sintax_problem(self):
        f = open('sample.py', 'a')
        f.write('\nimport some_not_imported_module\n')
        f.close()
        try:
            Utils.get_variables_from_file(os.path.abspath('sample.py'))
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("error interpreting config file 'sample.py': No module named some_not_imported_module", str(e))

    def test_it_should_raise_exception_config_file_not_exists(self):
        try:
            Utils.get_variables_from_file(os.path.abspath('unexistent.conf'))
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("%s: file not found" % os.path.abspath('unexistent.conf'), str(e))

    def test_it_should_delete_compiled_module_file(self):
        Utils.get_variables_from_file(os.path.abspath('sample.py'))
        self.assertFalse(os.path.exists(os.path.abspath('sample.pyc')))

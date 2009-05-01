from test import *
from core import *
from pmock import *
import os
import unittest

class ConfigTest(unittest.TestCase):
    
    def setUp(self):
        config_file = """
HOST = os.getenv("DB_HOST") or "localhost"
USERNAME = os.getenv("DB_USERNAME") or "root"
PASSWORD = os.getenv("DB_PASSWORD") or ""
DATABASE = os.getenv("DB_DATABASE") or "migration_example"
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR") or "example"
"""
        f = open("sample.conf", "w")
        f.write(config_file)
        f.close()
        
    def tearDown(self):
        os.remove("sample.conf")
    
    def test_it_should_read_config_file(self):
        config_path = os.path.abspath("sample.conf")
        config = Config(config_path)
        self.assertEquals(config.get("db_host"), "localhost")
        self.assertEquals(config.get("db_user"), "root")
        self.assertEquals(config.get("db_password"), "")
        self.assertEquals(config.get("db_name"), "migration_example")
        self.assertEquals(config.get("db_version_table"), "__db_version__")
        self.assertEquals(config.get("migrations_dir"), os.path.abspath("example"))

    def test_it_should_stop_execution_when_an_invalid_key_is_requested(self):
        config_path = os.path.abspath("sample.conf")
        config = Config(config_path)
        try:
            config.get("invalid_config")
            self.fail("it should not pass here")
        except:
            pass
    
    def test_it_should_create_new_configs(self):
        config_path = os.path.abspath("sample.conf")
        config = Config(config_path)
        
        # ensure that the config does not exist
        self.assertRaises(Exception, config.get, "sample_config", "TEST")
        
        # create the config
        config.put("sample_config", "TEST")
        
        # read the config
        self.assertEquals(config.get("sample_config"), "TEST")
        
    def test_it_should_not_override_existing_configs(self):
        config_path = os.path.abspath("sample.conf")
        config = Config(config_path)
        config.put("sample_config", "TEST")
        self.assertRaises(Exception, config.put, "sample_config", "TEST")

class MigrationsTest(unittest.TestCase):
    
    def __create_empty_file(self, file_name):
        f = open(file_name, "w")
        f.close()
        
    def __create_config(self):
        config_file = """
HOST = os.getenv("DB_HOST") or "localhost"
USERNAME = os.getenv("DB_USERNAME") or "root"
PASSWORD = os.getenv("DB_PASSWORD") or ""
DATABASE = os.getenv("DB_DATABASE") or "migration_example"
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR") or "."
"""
        f = open("test_config_file.conf", "w")
        f.write(config_file)
        f.close()
        
        self.__config = Config("test_config_file.conf")
            
    def setUp(self):
        self.__create_config()
        
        # random migration_files
        self.__test_migration_files = ["20090214115100_example_migration_file1.migration", 
                "20090214115200_example_migration_file2.migration", 
                "20090214115300_example_migration_file3.migration",
                "20090214115400_example_migration_file4.migration",
                "20090214115500_example_migration_file5.migration",
                "20090214115600_example_migration_file6.migration"]
        
        for each_file in self.__test_migration_files:
            self.__create_empty_file(each_file)
        
        # migration file with commands
        file_with_commands = "20090214120600_example_migration_file_with_commands.migration"
        f = open(file_with_commands, "w")
        f.write("SQL_UP = 'create table test;'\n")
        f.write("SQL_DOWN = 'drop table test;'\n")
        f.close()
        self.__test_migration_files.append(file_with_commands)
        
        # migration file without commands
        file_without_commands = "20090214120700_example_migration_file_without_commands.migration"
        f = open(file_without_commands, "w")
        f.write("SQL_UP = ''\n")
        f.write("SQL_DOWN = ''\n")
        f.close()
        self.__test_migration_files.append(file_without_commands)
        
        # very very last schema version available
        file_in_the_future = "21420101000000_example_migration_file.migration"
        self.__create_empty_file(file_in_the_future)
        self.__test_migration_files.append(file_in_the_future)
        
        # random migration files with bad names
        self.__test_migration_files_with_bad_names = ["randomrandomrandom.migration", 
                "21420101000000-wrong-separators.migration", 
                "2009021401_old_file_name_style.migration"
                "20090214120600_good_name_bad_extension.foo",
                "spamspamspamspamspaam"]
        
        for each_file in self.__test_migration_files_with_bad_names:
            self.__create_empty_file(each_file)
    
    def tearDown(self):
        for each_file in self.__test_migration_files:
            os.remove(each_file)

        for each_file in self.__test_migration_files_with_bad_names:
            os.remove(each_file)
        
        # eventually the tests that fail leave some garbage behind
        # this is to clean up the mess
        try:
            db_migrate = Migrations(self.__config)
            for each_file in db_migrate.get_all_migration_files():
                os.remove(each_file)
        except:
            pass
        
        os.remove("test_config_file.conf")

    def test_it_should_get_all_migration_files_in_dir(self):
        db_migrate = Migrations(self.__config)
        migration_files = db_migrate.get_all_migration_files()
        for each_file in migration_files:
            self.assertTrue(each_file in self.__test_migration_files)
            
    def test_it_should_get_only_valid_migration_files_in_dir(self):
        db_migrate = Migrations(self.__config)
        migration_files = db_migrate.get_all_migration_files()
        
        for file_name in self.__test_migration_files:
            self.assertTrue(file_name in migration_files)
            
        for bad_file_name in self.__test_migration_files_with_bad_names:
            self.assertFalse(bad_file_name in migration_files)
    
    def test_it_should_get_all_migration_versions_available(self):
        db_migrate = Migrations(self.__config)
        migration_files = db_migrate.get_all_migration_files()
        expected_versions = []
        for each_file in migration_files:
            expected_versions.append(db_migrate.get_migration_version(each_file))
        
        all_versions = db_migrate.get_all_migration_versions()
        for each_version_got in all_versions:
            self.assertTrue(each_version_got in expected_versions)
            
    def test_it_should_get_all_migration_versions_up_to_a_version(self):
        db_migrate = Migrations(self.__config)
        migration_files = db_migrate.get_all_migration_versions_up_to("20090214115200")
        self.assertEquals(len(migration_files), 1)
        self.assertEquals(migration_files[0], "20090214115100")    
        
    def test_it_should_get_migration_up_command_in_file(self):
        db_migrate = Migrations(self.__config)
        migration_file = "20090214120600_example_migration_file_with_commands.migration"
        sql = db_migrate.get_sql_command(migration_file, True)
        self.assertEquals(sql, "create table test;")
    
    def test_it_should_get_migration_down_command_in_file(self):
        db_migrate = Migrations(self.__config)
        migration_file = "20090214120600_example_migration_file_with_commands.migration"
        sql = db_migrate.get_sql_command(migration_file, False)
        self.assertEquals(sql, "drop table test;")
        
    def test_it_should_not_get_migration_command_in_files_with_blank_commands(self):
        db_migrate = Migrations(self.__config)
        migration_file = "20090214120700_example_migration_file_without_commands.migration"
        try:
            sql = db_migrate.get_sql_command(migration_file, True)
            self.fail("it should not pass here")
        except:
            pass

    def test_it_should_not_get_migration_command_in_empty_file(self):
        db_migrate = Migrations(self.__config)
        migration_file = self.__test_migration_files[0]
        try:
            sql = db_migrate.get_sql_command(migration_file, True)
            self.fail("it should not pass here")
        except NameError:
            self.fail("it should not pass here")
        except:
            pass
        
    def test_it_should_get_migration_version_from_file(self):
        db_migrate = Migrations(self.__config)
        # good file name
        example_file_name = "20090214120600_example_migration_file_name.migration"
        version = db_migrate.get_migration_version(example_file_name)
        self.assertEquals(version, "20090214120600")
        # old file name
        example_file_name = "2009021401_example_migration_file_name.migration"
        version = db_migrate.get_migration_version(example_file_name)
        self.assertEquals(version, "2009021401")
    
    def test_it_should_check_if_schema_version_exists_in_files(self):
        db_migrate = Migrations(self.__config)
        exists = db_migrate.check_if_version_exists("20090214115100")
        self.assertTrue(exists)
        
    def test_it_should_not_inform_that_schema_version_exists_just_matching_the_beggining_of_version_number(self):
        db_migrate = Migrations(self.__config)
        exists = db_migrate.check_if_version_exists("2009")
        self.assertFalse(exists)
    
    def test_it_should_get_the_latest_schema_version_available(self):
        db_migrate = Migrations(self.__config)
        latest_version = db_migrate.latest_schema_version_available()
        self.assertEquals(latest_version, "21420101000000")
        
    def test_it_should_validate_file_name_format_mask(self):
        db_migrate = Migrations(self.__config)
        
        for file_name in self.__test_migration_files:
            self.assertTrue(db_migrate.is_file_name_valid(file_name))
        
        for bad_file_name in self.__test_migration_files_with_bad_names:
            self.assertFalse(db_migrate.is_file_name_valid(bad_file_name))
    
    def test_it_should_not_validate_gedit_swap_files(self):
        db_migrate = Migrations(self.__config)
        invalid_file_name = "%s~" % self.__test_migration_files[0]
        self.assertFalse(db_migrate.is_file_name_valid(invalid_file_name))
            
    def test_it_should_create_migration_file(self):
        db_migrate = Migrations(self.__config)
        new_migration_file_name = db_migrate.create_migration("create_a_migration_file")
        
        self.assertTrue(db_migrate.is_file_name_valid(new_migration_file_name))
        self.assertTrue(os.path.exists(new_migration_file_name))
        
        # register file to be deleted
        self.__test_migration_files.append(new_migration_file_name)
        
    def test_it_should_not_create_migration_file_with_bad_name(self):
        db_migrate = Migrations(self.__config)
        try:
            db_migrate.create_migration("INVALID FILE NAME")
            self.fail("it should not pass here")
        except:
            pass
    
    def test_it_should_get_migration_file_from_version_number(self):
        db_migrate = Migrations(self.__config)
        migration_file_name = db_migrate.get_migration_file_name_from_version_number("20090214115100")
        self.assertEquals(migration_file_name, "20090214115100_example_migration_file1.migration")
        
    def test_it_should_get_none_migration_file_from_invalid_version_number(self):
        db_migrate = Migrations(self.__config)
        migration_file_name = db_migrate.get_migration_file_name_from_version_number("***invalid***")
        self.assertTrue(migration_file_name is None)
          
if __name__ == "__main__":
    unittest.main()

from test import *
from core import *
from pmock import *
import os
import unittest

class SimpleDBMigrateTest(unittest.TestCase):

    def __create_empty_file(self, file_name):
        f = open(file_name, "w")
        f.close()
            
    def setUp(self):
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
            db_migrate = SimpleDBMigrate(".")
            for each_file in db_migrate.get_all_migration_files():
                os.remove(each_file)
        except:
            pass

    def test_it_should_get_all_migration_files_in_dir(self):
        db_migrate = SimpleDBMigrate(".")
        migration_files = db_migrate.get_all_migration_files()
        for each_file in migration_files:
            self.assertTrue(each_file in self.__test_migration_files)
            
    def test_it_should_get_only_valid_migration_files_in_dir(self):
        db_migrate = SimpleDBMigrate(".")
        migration_files = db_migrate.get_all_migration_files()
        
        for file_name in self.__test_migration_files:
            self.assertTrue(file_name in migration_files)
            
        for bad_file_name in self.__test_migration_files_with_bad_names:
            self.assertFalse(bad_file_name in migration_files)
            
    def test_it_should_get_migration_up_command_in_file(self):
        db_migrate = SimpleDBMigrate(".")
        migration_file = "20090214120600_example_migration_file_with_commands.migration"
        sql = db_migrate.get_sql_command(migration_file, True)
        self.assertEquals(sql, "create table test;")
    
    def test_it_should_get_migration_down_command_in_file(self):
        db_migrate = SimpleDBMigrate(".")
        migration_file = "20090214120600_example_migration_file_with_commands.migration"
        sql = db_migrate.get_sql_command(migration_file, False)
        self.assertEquals(sql, "drop table test;")
        
    def test_it_should_get_migration_version_from_file(self):
        db_migrate = SimpleDBMigrate(".")
        # good file name
        example_file_name = "20090214120600_example_migration_file_name.migration"
        version = db_migrate.get_migration_version(example_file_name)
        self.assertEquals(version, "20090214120600")
        # old file name
        example_file_name = "2009021401_example_migration_file_name.migration"
        version = db_migrate.get_migration_version(example_file_name)
        self.assertEquals(version, "2009021401")
    
    def test_it_should_check_if_schema_version_exists_in_files(self):
        db_migrate = SimpleDBMigrate(".")
        exists = db_migrate.check_if_version_exists("20090214115100")
        self.assertTrue(exists)
        
    def test_it_should_get_all_migration_files_between_versions_when_migrating_up(self):
        db_migrate = SimpleDBMigrate(".")
        files = db_migrate.get_migration_files_between_versions("20090214115200", "20090214115500")
        
        expected_files = ["20090214115300_example_migration_file3.migration",
                "20090214115400_example_migration_file4.migration",
                "20090214115500_example_migration_file5.migration"]
        
        self.assertEquals(len(files), len(expected_files))
        
        for each_file in files:
            self.assertTrue(each_file in expected_files)
    
    def test_it_should_get_all_migration_files_between_versions_when_migrating_down(self):
        db_migrate = SimpleDBMigrate(".")
        files = db_migrate.get_migration_files_between_versions("20090214115500", "20090214115200")
        
        expected_files = ["20090214115500_example_migration_file5.migration", 
                "20090214115400_example_migration_file4.migration",
                "20090214115300_example_migration_file3.migration"]
        
        self.assertEquals(len(files), len(expected_files))
        
        for each_file in files:
            self.assertTrue(each_file in expected_files)
    
    def test_it_should_get_the_latest_schema_version_available(self):
        db_migrate = SimpleDBMigrate(".")
        latest_version = db_migrate.latest_schema_version_available()
        self.assertEquals(latest_version, "21420101000000")
        
    def test_it_should_validate_file_name_format_mask(self):
        db_migrate = SimpleDBMigrate(".")
        
        for file_name in self.__test_migration_files:
            self.assertTrue(db_migrate.is_file_name_valid(file_name))
        
        for bad_file_name in self.__test_migration_files_with_bad_names:
            self.assertFalse(db_migrate.is_file_name_valid(bad_file_name))
            
    def test_it_should_create_migration_file(self):
        db_migrate = SimpleDBMigrate(".")
        new_migration_file_name = db_migrate.create_migration("create_a_migration_file")
        
        self.assertTrue(db_migrate.is_file_name_valid(new_migration_file_name))
        self.assertTrue(os.path.exists(new_migration_file_name))
        
        # register file to be deleted
        self.__test_migration_files.append(new_migration_file_name)
        
    def test_it_should_not_create_migration_file_with_bad_name(self):
        db_migrate = SimpleDBMigrate(".")
        try:
            db_migrate.create_migration("INVALID FILE NAME")
            self.fail("it should not pass here")
        except:
            pass
            
if __name__ == "__main__":
    unittest.main()

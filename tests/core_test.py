from test import *
from core import *
from pmock import *
import unittest

class SimpleDBMigrateTest(unittest.TestCase):

    def __create_empty_file(self, file_name):
        f = open(file_name, "w")
        f.close()
            
    def setUp(self):
        # random migration_files
        self.__test_migration_files = ["20090214115100_example_migration_file1.sql", 
                "20090214115200_example_migration_file2.sql", 
                "20090214115300_example_migration_file3.sql",
                "20090214115400_example_migration_file4.sql",
                "20090214115500_example_migration_file5.sql",
                "20090214115600_example_migration_file6.sql"]
        
        for each_file in self.__test_migration_files:
            self.__create_empty_file(each_file)
        
        # migration file with commands
        file_with_commands = "20090214120600_example_migration_file_with_commands.sql"
        f = open(file_with_commands, "w")
        f.write("SQL_UP = 'create table test;'\n")
        f.write("SQL_DOWN = 'drop table test;'\n")
        f.close()
        self.__test_migration_files.append(file_with_commands)
        
        # very very last schema version available
        file_in_the_future = "21420101000000_example_migration_file.sql"
        self.__create_empty_file(file_in_the_future)
        self.__test_migration_files.append(file_in_the_future)
    
    def tearDown(self):
        for each_file in self.__test_migration_files:
            os.remove(each_file)

    def test_it_should_get_all_migration_files_in_dir(self):
        db_migrate = SimpleDBMigrate(".")
        migration_files = db_migrate.get_all_migration_files()
        for each_file in migration_files:
            self.assertTrue(each_file in self.__test_migration_files)
            
    def test_it_should_get_migration_up_command_in_file(self):
        db_migrate = SimpleDBMigrate(".")
        migration_file = "20090214120600_example_migration_file_with_commands.sql"
        sql = db_migrate.get_sql_command(migration_file, True)
        self.assertEquals(sql, "create table test;")
    
    def test_it_should_get_migration_down_command_in_file(self):
        db_migrate = SimpleDBMigrate(".")
        migration_file = "20090214120600_example_migration_file_with_commands.sql"
        sql = db_migrate.get_sql_command(migration_file, False)
        self.assertEquals(sql, "drop table test;")
        
    def test_it_should_get_migration_version_from_file(self):
        db_migrate = SimpleDBMigrate(".")
        # good file name
        example_file_name = "20090214120600_example_migration_file_name.sql"
        version = db_migrate.get_migration_version(example_file_name)
        self.assertEquals(version, "20090214120600")
        # old file name
        example_file_name = "2009021401_example_migration_file_name.sql"
        version = db_migrate.get_migration_version(example_file_name)
        self.assertEquals(version, "2009021401")
    
    def test_it_should_check_if_schema_version_exists_in_files(self):
        db_migrate = SimpleDBMigrate(".")
        exists = db_migrate.check_if_version_exists("20090214115100")
        self.assertTrue(exists)
        
    def test_it_should_get_all_migration_files_between_versions_when_migrating_up(self):
        db_migrate = SimpleDBMigrate(".")
        files = db_migrate.get_migration_files_between_versions("20090214115200", "20090214115500")
        
        expected_files = ["20090214115300_example_migration_file3.sql",
                "20090214115400_example_migration_file4.sql",
                "20090214115500_example_migration_file5.sql"]
        
        self.assertEquals(len(files), len(expected_files))
        
        for each_file in files:
            self.assertTrue(each_file in expected_files)
    
    def test_it_should_get_all_migration_files_between_versions_when_migrating_down(self):
        db_migrate = SimpleDBMigrate(".")
        files = db_migrate.get_migration_files_between_versions("20090214115500", "20090214115200")
        
        expected_files = ["20090214115500_example_migration_file5.sql", 
                "20090214115400_example_migration_file4.sql",
                "20090214115300_example_migration_file3.sql"]
        
        self.assertEquals(len(files), len(expected_files))
        
        for each_file in files:
            self.assertTrue(each_file in expected_files)
    
    def test_it_should_get_the_latest_schema_version_available(self):
        db_migrate = SimpleDBMigrate(".")
        latest_version = db_migrate.latest_schema_version_available()
        self.assertEquals(latest_version, "21420101000000")
    
if __name__ == "__main__":
    unittest.main()

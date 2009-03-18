from test import *
from main import *
from pmock import *
import unittest

class MainTest(unittest.TestCase):
    
    def setUp(self):
        self.database_versions = []
        self.database_versions.append("0")
        self.database_versions.append("20090211120000")
        self.database_versions.append("20090211120001")
        self.database_versions.append("20090211120002")
        self.database_versions.append("20090211120003")
        self.database_versions.append("20090212120000")
    
    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_up(self):
        database_versions = self.database_versions
        
        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")
        
        # mocking stuff
        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_all_schema_versions").will(return_value(database_versions))
        
        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("get_all_migration_versions").will(return_value(migration_files_versions))
        
        main = Main(mysql=mysql_mock, db_migrate=db_migrate_mock)
        
        # execute stuff
        migrations_to_be_executed = main._get_migration_files_to_be_executed("20090212120000", "20090212120005")
        
        self.assertEquals(len(migrations_to_be_executed), 3)
        self.assertEquals(migrations_to_be_executed[0], "20090211120005")
        self.assertEquals(migrations_to_be_executed[1], "20090211120006")
        self.assertEquals(migrations_to_be_executed[2], "20090212120005")
    
    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_up_and_current_destination_versions_are_the_same(self):
        database_versions = self.database_versions

        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")

        # mocking stuff
        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_all_schema_versions").will(return_value(database_versions))

        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("get_all_migration_versions").will(return_value(migration_files_versions))

        main = Main(mysql=mysql_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main._get_migration_files_to_be_executed("20090212120000", "20090212120000")

        self.assertEquals(len(migrations_to_be_executed), 2)
        self.assertEquals(migrations_to_be_executed[0], "20090211120005")
        self.assertEquals(migrations_to_be_executed[1], "20090211120006")

    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_up_and_current_destination_versions_are_the_same_and_migration_versions_are_higher_than_database_versions(self):
        database_versions = self.database_versions

        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090212120001")
        migration_files_versions.append("20090212120002")

        # mocking stuff
        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_all_schema_versions").will(return_value(database_versions))

        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("get_all_migration_versions").will(return_value(migration_files_versions))

        main = Main(mysql=mysql_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main._get_migration_files_to_be_executed("20090212120000", "20090212120000")

        self.assertEquals(len(migrations_to_be_executed), 0)
    
    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_down(self):
        database_versions = self.database_versions
        migration_files_versions = self.database_versions[:] #copy

        # mocking stuff
        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_all_schema_versions").will(return_value(database_versions))

        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("get_all_migration_versions").will(return_value(migration_files_versions))

        main = Main(mysql=mysql_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main._get_migration_files_to_be_executed("20090212120000", "20090211120001")

        self.assertEquals(len(migrations_to_be_executed), 3)
        self.assertEquals(migrations_to_be_executed[0], "20090212120000")
        self.assertEquals(migrations_to_be_executed[1], "20090211120003")
        self.assertEquals(migrations_to_be_executed[2], "20090211120002")
        
    def test_it_should_show_an_error_message_if_tries_to_migrate_down_and_migration_file_does_not_exists(self):
        database_versions = self.database_versions
        migration_files_versions = [] #empty
    
        # mocking stuff
        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_all_schema_versions").will(return_value(database_versions))

        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("get_all_migration_versions").will(return_value(migration_files_versions))

        main = Main(mysql=mysql_mock, db_migrate=db_migrate_mock)

        # execute stuff
        try:
            migrations_to_be_executed = main._get_migration_files_to_be_executed("20090212120000", "20090211120001")
            self.fail("it should not pass here")
        except:
            pass
    
if __name__ == "__main__":
    unittest.main()

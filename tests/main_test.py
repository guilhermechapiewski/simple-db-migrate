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

    def test_it_should_create_migration_if_option_is_activated_by_the_user(self):
        class MainMock(Main):
            def create_migration(self):
                assert True
            def migrate(self):
                assert False, "it should not try to migrate database!"
        
        config_mock = {"new_migration":"some_new_migration"}
        mysql_mock = Mock()
        db_migrate_mock = Mock()
        main = MainMock(config=config_mock, mysql=mysql_mock, db_migrate=db_migrate_mock)
        main.execute()

    def test_it_should_migrate_db_if_create_migration_option_is_not_activated_by_user(self):
        class MainMock(Main):
            def create_migration(self):
                assert False, "it should not try to migrate database!"
            def migrate(self):
                assert True

        mysql_mock = Mock()
        db_migrate_mock = Mock()
        main = MainMock(config={}, mysql=mysql_mock, db_migrate=db_migrate_mock)
        main.execute()
        
    def test_it_should_create_new_migration(self):
        import core
        core.Migration = Mock()
        core.Migration.TEMPLATE = ""
        core.Migration.expects(once()).method("is_file_name_valid").will(return_value(True))
        core.Migration.expects(once()).method("create").with(eq("some_new_migration"))
        
        config_mock = {"new_migration":"some_new_migration"}
        mysql_mock = Mock()
        db_migrate_mock = Mock()
        main = Main(config=config_mock, mysql=mysql_mock, db_migrate=db_migrate_mock)
        main.execute()
    
    def test_it_should_migrate_database_with_migration_is_up(self):
        class MainMock(Main):
            def get_destination_version(self):
                return "20090810170301"
            def execute_migrations(self, current_version, destination_version, is_migration_up):
                assert current_version == "20090810170300"
                assert destination_version == "20090810170301"
                assert is_migration_up
            
        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_current_schema_version").will(return_value("20090810170300"))
        db_migrate_mock = Mock()
        main = MainMock(config={}, mysql=mysql_mock, db_migrate=db_migrate_mock)
        main.execute()

    def test_it_should_migrate_database_with_migration_is_down(self):
        class MainMock(Main):
            def get_destination_version(self):
                return "20080810170300"
            def execute_migrations(self, current_version, destination_version, is_migration_up):
                assert current_version == "20090810170300"
                assert destination_version == "20080810170300"
                assert not is_migration_up

        mysql_mock = Mock()
        mysql_mock.expects(once()).method("get_current_schema_version").will(return_value("20090810170300"))
        db_migrate_mock = Mock()
        main = MainMock(config={}, mysql=mysql_mock, db_migrate=db_migrate_mock)
        main.execute()
        
    def test_it_should_get_destination_version_when_user_informs_a_specific_version(self):
        config_mock = {"schema_version":"20090810170300"}
        mysql_mock = Mock()
        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("check_if_version_exists").with(eq("20090810170300")).will(return_value(True))
        main = Main(config=config_mock, mysql=mysql_mock, db_migrate=db_migrate_mock)
        destination_version = main.get_destination_version()
        assert destination_version == "20090810170300"

    def test_it_should_get_destination_version_when_user_does_not_inform_a_specific_version(self):
        mysql_mock = Mock()
        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("latest_version_available").will(return_value("20090810170300"))
        db_migrate_mock.expects(once()).method("check_if_version_exists").with(eq("20090810170300")).will(return_value(True))
        main = Main(config={}, mysql=mysql_mock, db_migrate=db_migrate_mock)
        destination_version = main.get_destination_version()
        assert destination_version == "20090810170300"

    def test_it_should_raise_exception_when_get_destination_version_and_version_does_not_exist(self):
        mysql_mock = Mock()
        db_migrate_mock = Mock()
        db_migrate_mock.expects(once()).method("latest_version_available").will(return_value("20090810170300"))
        db_migrate_mock.expects(once()).method("check_if_version_exists").with(eq("20090810170300")).will(return_value(False))
        main = Main(config={}, mysql=mysql_mock, db_migrate=db_migrate_mock)
        self.assertRaises(Exception, main.get_destination_version)

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
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120005")
        
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
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120000")

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
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120000")

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
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090211120001")

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
            migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090211120001")
            self.fail("it should not pass here")
        except:
            pass
    
if __name__ == "__main__":
    unittest.main()

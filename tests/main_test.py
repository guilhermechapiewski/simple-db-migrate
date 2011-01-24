from mox import Mox
import mox as module_mox
import unittest
import os

from simple_db_migrate.main import *
from simple_db_migrate.core import Migration, SimpleDBMigrate
from simple_db_migrate.config import *

class MainTest(unittest.TestCase):

    def setUp(self):
        self.database_versions = []
        self.database_versions.append("0")
        self.database_versions.append("20090211120000")
        self.database_versions.append("20090211120001")
        self.database_versions.append("20090211120002")
        self.database_versions.append("20090211120003")
        self.database_versions.append("20090212120000")

        self.migration_20090211120002 = Migration(id=12, version="20090211120002", file_name="20090211120002", sql_up="sql_up", sql_down="sql_down")
        self.migration_20090211120003 = Migration(id=13, version="20090211120003", file_name="20090211120003", sql_up="sql_up", sql_down="sql_down")
        self.migration_20090212120000 = Migration(id=14, version="20090212120000", file_name="20090212120000", sql_up="sql_up", sql_down="sql_down")
        self.migration_20090211120005 = Migration(id=15, version="20090211120005", file_name="20090211120005", sql_up="sql_up", sql_down="sql_down")
        self.migration_20090211120006 = Migration(id=16, version="20090211120006", file_name="20090211120006", sql_up="sql_up", sql_down="sql_down")
        self.migration_20090212120005 = Migration(id=17, version="20090212120005", file_name="20090212120005", sql_up="sql_up", sql_down="sql_down")

        config_file = """
HOST = os.getenv("DB_HOST") or "localhost"
USERNAME = os.getenv("DB_USERNAME") or "root"
PASSWORD = os.getenv("DB_PASSWORD") or ""
DATABASE = os.getenv("DB_DATABASE") or "migration_test"
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR") or "."
"""
        f = open("test.conf", "w")
        f.write(config_file)
        f.close()

        self.config = FileConfig("test.conf")
        self.config.put('log_dir', None)

    def tearDown(self):
        os.remove("test.conf")

    def test_it_should_create_migration_if_option_is_activated_by_the_user(self):
        class MainMock(Main):
            def create_migration(self):
                assert True
            def migrate(self):
                assert False, "it should not try to migrate database!"

        self.config.put("new_migration","some_new_migration")

        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        db_migrate_mock = mox.CreateMockAnything()

        mox.ReplayAll()

        main = MainMock(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        main.execute()

        mox.VerifyAll()

    def test_it_should_migrate_db_if_create_migration_option_is_not_activated_by_user(self):
        class MainMock(Main):
            def create_migration(self):
                assert False, "it should try to migrate database!"
            def migrate(self):
                assert True

        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        db_migrate_mock = mox.CreateMockAnything()

        mox.ReplayAll()

        main = MainMock(config={}, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        main.execute()

        mox.VerifyAll()

    def test_it_should_create_new_migration(self):
        from simple_db_migrate import core
        from time import strftime

        original_migration_module = core.Migration
        file_created = None
        try:
            mox = Mox()
            core.Migration = mox.CreateMock(Migration)
            file_created = '%s_some_new_migration.migration' % strftime("%Y%m%d%H%M%S")
            core.Migration.is_file_name_valid(file_created).AndReturn(True)

            self.config.put("new_migration","some_new_migration")

            sgdb_mock = mox.CreateMockAnything()
            db_migrate_mock = mox.CreateMockAnything()

            mox.ReplayAll()

            main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
            main.execute()

            mox.VerifyAll()
        finally:
            core.Migration = original_migration_module
            if file_created and os.path.isfile(file_created):
                os.remove(file_created)

    def test_it_should_migrate_database_with_migration_is_up(self):
        class MainMock(Main):
            def get_destination_version(self):
                return "20090810170301"
            def execute_migrations(self, current_version, destination_version):
                assert current_version == "20090810170300"
                assert destination_version == "20090810170301"

        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_current_schema_version().AndReturn("20090810170300")

        db_migrate_mock = mox.CreateMockAnything()

        mox.ReplayAll()

        main = MainMock(config={}, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        main.execute()

        mox.VerifyAll()

    def test_it_should_migrate_database_with_migration_is_down(self):
        class MainMock(Main):
            def get_destination_version(self):
                return "20080810170300"
            def execute_migrations(self, current_version, destination_version):
                assert current_version == "20090810170300"
                assert destination_version == "20080810170300"

        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_current_schema_version().AndReturn("20090810170300")

        db_migrate_mock = mox.CreateMockAnything()

        mox.ReplayAll()

        main = MainMock(config={}, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        main.execute()

        mox.VerifyAll()

    def test_it_should_get_destination_version_when_user_informs_a_specific_version(self):
        self.config.put("schema_version","20090810170300")

        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_id_from_version_number('20090810170300').AndReturn(None)
        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.check_if_version_exists("20090810170300").AndReturn(True)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        destination_version = main.get_destination_version()
        assert destination_version == "20090810170300"

        mox.VerifyAll()

    def test_it_should_get_destination_version_when_user_does_not_inform_a_specific_version(self):

        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.latest_version_available().AndReturn("20090810170300")
        db_migrate_mock.check_if_version_exists("20090810170300").AndReturn(True)

        mox.ReplayAll()

        main = Main(config={}, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        destination_version = main.get_destination_version()
        assert destination_version == "20090810170300"

        mox.VerifyAll()

    def test_it_should_raise_exception_when_get_destination_version_and_version_does_not_exist(self):
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_id_from_version_number('20090810170300').AndReturn(None)
        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.latest_version_available().AndReturn("20090810170300")
        db_migrate_mock.check_if_version_exists("20090810170300").AndReturn(False)

        mox.ReplayAll()

        main = Main(config={}, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        try:
            main.get_destination_version()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "version not found (20090810170300)")

        mox.VerifyAll()

    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_up(self):
        database_versions = self.database_versions[:]
        is_migration_up = True

        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)
        db_migrate_mock.get_migration_from_version_number('20090211120005').AndReturn(self.migration_20090211120005)
        db_migrate_mock.get_migration_from_version_number('20090211120006').AndReturn(self.migration_20090211120006)
        db_migrate_mock.get_migration_from_version_number('20090212120005').AndReturn(self.migration_20090212120005)

        mox.ReplayAll()

        main = Main(sgdb=sgdb_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120005", is_migration_up)

        self.assertEquals(len(migrations_to_be_executed), 3)
        self.assertEquals(migrations_to_be_executed[0].version, "20090211120005")
        self.assertEquals(migrations_to_be_executed[1].version, "20090211120006")
        self.assertEquals(migrations_to_be_executed[2].version, "20090212120005")

        mox.VerifyAll()

    def test_it_should_not_get_any_migration_files_to_be_executed_considering_database_version_when_migrating_up_and_current_destination_versions_are_the_same(self):
        database_versions = self.database_versions[:]
        is_migration_up = True

        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()

        db_migrate_mock = mox.CreateMockAnything()

        mox.ReplayAll()

        main = Main(sgdb=sgdb_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120000", is_migration_up)

        self.assertEquals(len(migrations_to_be_executed), 0)

        mox.VerifyAll()

    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_up_and_current_destination_versions_are_the_same(self):
        self.config.put("force_execute_old_migrations_versions",True)
        database_versions = self.database_versions[:]
        is_migration_up = True

        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)
        db_migrate_mock.get_migration_from_version_number('20090211120005').AndReturn(self.migration_20090211120005)
        db_migrate_mock.get_migration_from_version_number('20090211120006').AndReturn(self.migration_20090211120006)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)

        # execute stuff
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120000", is_migration_up)

        self.assertEquals(len(migrations_to_be_executed), 2)
        self.assertEquals(migrations_to_be_executed[0].version, "20090211120005")
        self.assertEquals(migrations_to_be_executed[1].version, "20090211120006")

        mox.VerifyAll()

    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_up_and_current_destination_versions_are_the_same_and_migration_versions_are_higher_than_database_versions(self):
        database_versions = self.database_versions[:]
        is_migration_up = True

        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090212120001")
        migration_files_versions.append("20090212120002")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()

        db_migrate_mock = mox.CreateMockAnything()

        mox.ReplayAll()

        main = Main(sgdb=sgdb_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090212120000", is_migration_up)

        self.assertEquals(len(migrations_to_be_executed), 0)

        mox.VerifyAll()

    def test_it_should_get_all_migration_files_that_must_be_executed_considering_database_version_when_migrating_down(self):
        database_versions = self.database_versions[:]
        migration_files_versions = self.database_versions[:] #copy
        is_migration_up = False

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120001').AndReturn(3)
        sgdb_mock.get_all_schema_migrations().AndReturn([self.migration_20090211120002, self.migration_20090211120003, self.migration_20090212120000])

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(sgdb=sgdb_mock, db_migrate=db_migrate_mock)

        # execute stuff
        migrations_to_be_executed = main.get_migration_files_to_be_executed("20090212120000", "20090211120001", is_migration_up)

        self.assertEquals(len(migrations_to_be_executed), 3)
        self.assertEquals(migrations_to_be_executed[0].version, "20090212120000")
        self.assertEquals(migrations_to_be_executed[1].version, "20090211120003")
        self.assertEquals(migrations_to_be_executed[2].version, "20090211120002")

        mox.VerifyAll()

    def test_it_should_show_an_error_message_if_tries_to_migrate_down_and_migration_file_does_not_exists_and_is_on_database_with_sql_down_empty(self):
        database_versions = self.database_versions[:]
        migration_files_versions = [] #empty
        is_migration_up = False

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120001').AndReturn(1)
        migration = Migration(id=12, version="20090212120000", file_name="20090212120000", sql_up="sql_up", sql_down="")
        sgdb_mock.get_all_schema_migrations().AndReturn([migration])

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        try:
            main.get_migration_files_to_be_executed("20090212120000", "20090211120001", is_migration_up)
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "impossible to migrate down: one of the versions was not found (20090212120000)")

        mox.VerifyAll()

    def test_it_should_fail_if_try_to_do_migration_to_a_non_existing_migration_file_or_database_version(self):
        self.config.put("schema_version","20090211120006")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_id_from_version_number('20090211120006').AndReturn(None)
        sgdb_mock.get_version_id_from_version_number('20090211120006').AndReturn(None)

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.check_if_version_exists('20090211120006').AndReturn(False)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "version not found (20090211120006)")

        mox.VerifyAll()

    def test_it_should_success_if_try_to_do_migration_up_to_a_existing_migration_file(self):
        self.config.put("schema_version","20090212120006")
        database_versions = self.database_versions[:]
        migration_files_versions = database_versions[:]
        migration_files_versions.append("20090212120006")

        class MainMock(Main):
            def get_migration_files_to_be_executed(self, current_version, destination_version, is_migration_up):
                assert current_version == "20090212120000"
                assert destination_version == "20090212120006"
                assert is_migration_up == True

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_id_from_version_number('20090212120006').AndReturn(None)
        sgdb_mock.get_current_schema_version().AndReturn("20090212120000")
        sgdb_mock.get_version_id_from_version_number('20090212120006').AndReturn(None)

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.check_if_version_exists('20090212120006').AndReturn(True)

        mox.ReplayAll()

        main = MainMock(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock)
        main.execute()

        mox.VerifyAll()

    def test_it_should_fail_if_try_to_do_migration_down_and_one_of_the_migrations_to_execute_is_a_non_existing_migration_file_and_is_on_database_with_sql_down_empty(self):
        self.config.put("schema_version","20090211120003")

        database_versions = self.database_versions[:]

        migration_files_versions = []

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)
        sgdb_mock.get_current_schema_version().AndReturn("20090211120005")
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)
        sgdb_mock.get_version_id_from_version_number('20090211120005').AndReturn(self.migration_20090211120005.id)
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)

        migration3 = Migration(id=13, version="20090211120003", sql_down=None)
        migration5 = Migration(id=15, version="20090211120005", sql_down=None)
        sgdb_mock.get_all_schema_migrations().AndReturn([migration3, migration5])

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.check_if_version_exists('20090211120003').AndReturn(False)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "impossible to migrate down: one of the versions was not found (20090211120005)")

        mox.VerifyAll()

    def test_it_should_pass_if_try_to_do_migration_down_to_a_non_existing_migration_file_wich_is_on_database_with_sql_down_empty(self):
        self.config.put("schema_version","20090211120003")
        self.config.put("show_sql_only",True)

        database_versions = self.database_versions[:]

        migration_files_versions = []

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)
        sgdb_mock.get_current_schema_version().AndReturn("20090211120005")
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)
        sgdb_mock.get_version_id_from_version_number('20090211120005').AndReturn(self.migration_20090211120005.id)
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)

        migration3 = Migration(id=13, version="20090211120003", sql_down=None)
        migration5 = Migration(id=15, version="20090211120005", sql_down="sql_down")
        sgdb_mock.get_all_schema_migrations().AndReturn([migration3, migration5])

        db_migrate_mock = mox.CreateMockAnything()
        db_migrate_mock.check_if_version_exists('20090211120003').AndReturn(False)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_label_last_executed_migration_if_a_label_was_specified_and_a_version_was_not_specified_and_label_is_not_present_at_database(self):
        self.config.put("label_version","experimental_version")
        self.config.put("schema_version",None)

        database_versions = self.database_versions[:]

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('experimental_version').AndReturn(None)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.change(module_mox.IsA(str), "20090211120006", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090212120005", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, "experimental_version")

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.latest_version_available().AndReturn(migration_files_versions[-1])
        db_migrate_mock.check_if_version_exists(migration_files_versions[-1]).AndReturn(True)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)
        db_migrate_mock.get_migration_from_version_number('20090211120006').AndReturn(self.migration_20090211120006)
        db_migrate_mock.get_migration_from_version_number('20090212120005').AndReturn(self.migration_20090212120005)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_label_last_executed_migration_if_a_label_was_specified_and_a_version_was_not_specified_and_label_is_not_present_at_database_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","experimental_version")
        self.config.put("schema_version",None)
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120000")
        database_versions.append("20090211120001")
        database_versions.append("20090212120000")

        migration_files_versions = self.database_versions[:]

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('experimental_version').AndReturn(None)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.change(module_mox.IsA(str), '20090211120002', module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), '20090211120003', module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, "experimental_version")


        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.latest_version_available().AndReturn(migration_files_versions[-1])
        db_migrate_mock.check_if_version_exists(migration_files_versions[-1]).AndReturn(True)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)
        db_migrate_mock.get_migration_from_version_number('20090211120002').AndReturn(self.migration_20090211120002)
        db_migrate_mock.get_migration_from_version_number('20090211120003').AndReturn(self.migration_20090211120003)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_do_migration_down_if_a_label_was_specified_and_a_version_was_not_specified_and_label_is_present_at_database(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version",None)

        database_versions = self.database_versions[:]

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_version_id_from_version_number('20090212120000').AndReturn(self.migration_20090212120000.id)
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)

        sgdb_mock.get_all_schema_migrations().AndReturn([self.migration_20090211120002, self.migration_20090211120003, self.migration_20090212120000])
        sgdb_mock.change(module_mox.IsA(str), "20090212120000", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120003", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.check_if_version_exists('20090211120002').AndReturn(True)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_do_migration_down_if_a_label_was_specified_and_a_version_was_not_specified_and_label_is_present_at_database_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version",None)
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120002")
        database_versions.append("20090211120003")
        database_versions.append("20090212120000")

        migration_files_versions = self.database_versions[:]

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_version_id_from_version_number('20090212120000').AndReturn(self.migration_20090212120000.id)
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)

        sgdb_mock.get_all_schema_migrations().AndReturn([self.migration_20090211120002, self.migration_20090211120003, self.migration_20090212120000])
        sgdb_mock.change(module_mox.IsA(str), "20090212120000", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120003", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.check_if_version_exists('20090211120002').AndReturn(True)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_label_last_executed_migration_if_a_label_and_a_version_were_specified_and_neither_them_are_present_at_database(self):
        self.config.put("label_version","experimental_version")
        self.config.put("schema_version","20090211120006")

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120000")
        database_versions.append("20090211120001")
        database_versions.append("20090211120002")
        database_versions.append("20090211120003")

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('experimental_version').AndReturn(None)
        sgdb_mock.get_version_id_from_version_number('20090211120006').AndReturn(None)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_version_id_from_version_number('20090211120006').AndReturn(None)

        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.change(module_mox.IsA(str), "20090211120005", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120006", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, "experimental_version")

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.check_if_version_exists('20090211120006').AndReturn(True)

        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)
        db_migrate_mock.get_migration_from_version_number('20090211120005').AndReturn(self.migration_20090211120005)
        db_migrate_mock.get_migration_from_version_number('20090211120006').AndReturn(self.migration_20090211120006)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_label_last_executed_migration_if_a_label_and_a_version_were_specified_and_neither_them_are_present_at_database_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","experimental_version")
        self.config.put("schema_version","20090211120006")
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120000")
        database_versions.append("20090211120001")
        database_versions.append("20090211120003")

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120005")
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('experimental_version').AndReturn(None)
        sgdb_mock.get_version_id_from_version_number('20090211120006').AndReturn(None)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_version_id_from_version_number('20090211120006').AndReturn(None)

        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.change(module_mox.IsA(str), "20090211120002", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120005", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120006", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), True, self.log, "experimental_version")

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.check_if_version_exists('20090211120006').AndReturn(True)

        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)
        db_migrate_mock.get_migration_from_version_number('20090211120002').AndReturn(self.migration_20090211120002)
        db_migrate_mock.get_migration_from_version_number('20090211120005').AndReturn(self.migration_20090211120005)
        db_migrate_mock.get_migration_from_version_number('20090211120006').AndReturn(self.migration_20090211120006)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_do_migration_down_if_a_label_and_a_version_were_specified_and_both_of_them_are_present_at_database_and_correspond_to_same_migration(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120002")

        database_versions = self.database_versions[:]

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_version_id_from_version_number('20090212120000').AndReturn(self.migration_20090212120000.id)
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)

        sgdb_mock.get_all_schema_migrations().AndReturn([self.migration_20090211120002, self.migration_20090211120003, self.migration_20090212120000])
        sgdb_mock.change(module_mox.IsA(str), "20090212120000", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120003", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.check_if_version_exists('20090211120002').AndReturn(True)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_do_migration_down_if_a_label_and_a_version_were_specified_and_both_of_them_are_present_at_database_and_correspond_to_same_migration_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120002")
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120002")
        database_versions.append("20090211120003")
        database_versions.append("20090212120000")

        migration_files_versions = self.database_versions[:]

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_current_schema_version().AndReturn(database_versions[-1])
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)
        sgdb_mock.get_version_id_from_version_number('20090212120000').AndReturn(self.migration_20090212120000.id)
        sgdb_mock.get_all_schema_versions().AndReturn(database_versions)
        sgdb_mock.get_version_id_from_version_number('20090211120002').AndReturn(self.migration_20090211120002.id)

        sgdb_mock.get_all_schema_migrations().AndReturn([self.migration_20090211120002, self.migration_20090211120003, self.migration_20090212120000])
        sgdb_mock.change(module_mox.IsA(str), "20090212120000", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)
        sgdb_mock.change(module_mox.IsA(str), "20090211120003", module_mox.IsA(str), module_mox.IsA(str), module_mox.IsA(str), False, self.log, None)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)
        db_migrate_mock.check_if_version_exists('20090211120002').AndReturn(True)
        db_migrate_mock.get_all_migration_versions().AndReturn(migration_files_versions)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        main.execute()

        mox.VerifyAll()

    def test_it_should_raise_error_if_a_label_and_a_version_were_specified_and_both_of_them_are_present_at_database_and_not_correspond_to_same_migration(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120003")

        database_versions = self.database_versions[:]

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "label (labeled_version) and schema_version (20090211120003) don't correspond to the same version at database")

        mox.VerifyAll()


    def test_it_should_raise_error_if_a_label_and_a_version_were_specified_and_both_of_them_are_present_at_database_and_not_correspond_to_same_migration_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120003")
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120002")
        database_versions.append("20090211120003")
        database_versions.append("20090212120000")

        migration_files_versions = self.database_versions[:]

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "label (labeled_version) and schema_version (20090211120003) don't correspond to the same version at database")

        mox.VerifyAll()

    def test_it_should_raise_error_if_a_label_and_a_version_were_specified_and_label_is_present_at_database_but_version_not(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120003")

        database_versions = self.database_versions[:]

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(None)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "label (labeled_version) or schema_version (20090211120003), only one of them exists in the database")

        mox.VerifyAll()

    def test_it_should_raise_error_if_a_label_and_a_version_were_specified_and_label_is_present_at_database_but_version_not_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120003")
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120002")
        database_versions.append("20090211120003")
        database_versions.append("20090212120000")

        migration_files_versions = self.database_versions[:]

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(self.migration_20090211120002.version)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(None)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "label (labeled_version) or schema_version (20090211120003), only one of them exists in the database")

        mox.VerifyAll()

    def test_it_should_raise_error_if_a_label_and_a_version_were_specified_and_version_is_present_at_database_but_label_not(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120003")

        database_versions = self.database_versions[:]

        migration_files_versions = self.database_versions[:]
        migration_files_versions.append("20090211120006")
        migration_files_versions.append("20090212120005")

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(None)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "label (labeled_version) or schema_version (20090211120003), only one of them exists in the database")

        mox.VerifyAll()


    def test_it_should_raise_error_if_a_label_and_a_version_were_specified_and_version_is_present_at_database_but_label_not_and_force_execute_old_migrations_versions_is_setted(self):
        self.config.put("label_version","labeled_version")
        self.config.put("schema_version","20090211120003")
        self.config.put("force_execute_old_migrations_versions",True)

        database_versions = []
        database_versions.append("0")
        database_versions.append("20090211120002")
        database_versions.append("20090211120003")
        database_versions.append("20090212120000")

        migration_files_versions = self.database_versions[:]

        self.migration_20090211120002.label = "labeled_version"

        # mocking stuff
        mox = Mox()
        sgdb_mock = mox.CreateMockAnything()
        sgdb_mock.get_version_number_from_label('labeled_version').AndReturn(None)
        sgdb_mock.get_version_id_from_version_number('20090211120003').AndReturn(self.migration_20090211120003.id)

        db_migrate_mock = mox.CreateMock(SimpleDBMigrate)

        mox.ReplayAll()

        main = Main(config=self.config, sgdb=sgdb_mock, db_migrate=db_migrate_mock, execution_log = self.log)
        try:
            main.execute()
            self.fail("it should not pass here")
        except AssertionError, e:
            raise e
        except Exception, e:
            self.assertEqual(str(e), "label (labeled_version) or schema_version (20090211120003), only one of them exists in the database")

        mox.VerifyAll()

    def log(self, msg, color="CYAN", log_level_limit=2):
        pass

if __name__ == "__main__":
    unittest.main()

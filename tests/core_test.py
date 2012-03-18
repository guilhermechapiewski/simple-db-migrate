# coding: utf-8
import os
import time
import unittest
from mock import patch, Mock
from simple_db_migrate.config import *
from simple_db_migrate.core import *
from tests import BaseTest, create_file, create_migration_file, delete_files, create_config

class SimpleDBMigrateTest(BaseTest):

    def setUp(self):
        if not os.path.exists(os.path.abspath('migrations')):
            os.mkdir(os.path.abspath('migrations'))
        self.config = create_config(migrations_dir='.:migrations')
        self.test_migration_files = []

        self.test_migration_files.append(os.path.abspath(create_migration_file('20090214115100_01_test_migration.migration', 'foo', 'bar')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('migrations/20090214115200_02_test_migration.migration', 'foo', 'bar')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('20090214115300_03_test_migration.migration', 'foo', 'bar')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('20090214115400_04_test_migration.migration', 'foo', 'bar')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('migrations/20090214115500_05_test_migration.migration', 'foo', 'bar')))
        self.test_migration_files.append(os.path.abspath(create_migration_file('migrations/20090214115600_06_test_migration.migration', 'foo', 'bar')))

    def test_it_should_use_migrations_dir_from_configuration(self):
        db_migrate = SimpleDBMigrate(self.config)
        self.assertEqual(self.config.get("database_migrations_dir"), db_migrate._migrations_dir)

    def test_it_should_use_script_encoding_from_configuration(self):
        db_migrate = SimpleDBMigrate(self.config)
        self.assertEqual(self.config.get('database_script_encoding'), db_migrate._script_encoding)

    def test_it_should_use_utf8_as_default_script_encoding_from_configuration(self):
        self.config.remove('database_script_encoding')
        db_migrate = SimpleDBMigrate(self.config)
        self.assertEqual('utf-8', db_migrate._script_encoding)

    def test_it_should_get_all_migrations_in_dir(self):
        db_migrate = SimpleDBMigrate(self.config)
        migrations = db_migrate.get_all_migrations()
        self.assertNotEqual(None, migrations)
        self.assertEqual(len(self.test_migration_files), len(migrations))
        for migration in migrations:
            self.assertTrue(migration.abspath in self.test_migration_files)

    @patch('simple_db_migrate.core.Migration.is_file_name_valid')
    def test_it_should_get_only_valid_migrations_in_dir(self, is_file_name_valid_mock):
        def side_effect(args):
            return args != '20090214115100_01_test_migration.migration'
        is_file_name_valid_mock.side_effect = side_effect

        db_migrate = SimpleDBMigrate(self.config)
        migrations = db_migrate.get_all_migrations()
        self.assertEqual(len(self.test_migration_files) - 1, len(migrations))
        for migration in migrations:
            self.assertTrue(migration.abspath in self.test_migration_files)
            self.assertFalse(migration.file_name == '20090214115100_01_test_migration.migration')

        self.assertEqual((len(self.test_migration_files) * 2) - 1, is_file_name_valid_mock.call_count)

    @patch('simple_db_migrate.core.Migration.is_file_name_valid', return_value=True)
    def test_it_should_not_read_files_again_on_subsequent_calls(self, is_file_name_valid_mock):
        db_migrate = SimpleDBMigrate(self.config)
        db_migrate.get_all_migrations()
        self.assertEqual((len(self.test_migration_files) * 2), is_file_name_valid_mock.call_count)

        #make the second call
        db_migrate.get_all_migrations()
        self.assertEqual((len(self.test_migration_files) * 2), is_file_name_valid_mock.call_count)

    def test_it_should_raise_error_if_has_an_invalid_dir_on_migrations_dir_list(self):
        self.config.update("database_migrations_dir", ['invalid_path_it_does_not_exist'])
        db_migrate = SimpleDBMigrate(self.config)
        self.assertRaisesWithMessage(Exception, "directory not found ('%s')" % os.path.abspath('invalid_path_it_does_not_exist'), db_migrate.get_all_migrations)

    @patch('simple_db_migrate.core.Migration.is_file_name_valid', return_value=False)
    def test_it_should_raise_error_if_do_not_have_any_valid_migration(self, is_file_name_valid_mock):
        db_migrate = SimpleDBMigrate(self.config)
        self.assertRaisesWithMessage(Exception, "no migration files found", db_migrate.get_all_migrations)

    def test_it_should_get_all_migration_versions_available(self):
        db_migrate = SimpleDBMigrate(self.config)
        migrations = db_migrate.get_all_migrations()
        expected_versions = []
        for migration in migrations:
            expected_versions.append(migration.version)

        all_versions = db_migrate.get_all_migration_versions()

        self.assertEqual(len(all_versions), len(expected_versions))
        for version in all_versions:
            self.assertTrue(version in expected_versions)

    @patch('simple_db_migrate.core.SimpleDBMigrate.get_all_migrations', return_value=[])
    def test_it_should_use_get_all_migrations_method_to_get_all_migration_versions_available(self, get_all_migrations_mock):
        db_migrate = SimpleDBMigrate(self.config)
        db_migrate.get_all_migration_versions()
        self.assertEqual(1, get_all_migrations_mock.call_count)

    def test_it_should_get_all_migration_versions_up_to_a_version(self):
        db_migrate = SimpleDBMigrate(self.config)
        migration_versions = db_migrate.get_all_migration_versions_up_to('20090214115200')
        self.assertEqual(1, len(migration_versions))
        self.assertEqual('20090214115100', migration_versions[0])

    @patch('simple_db_migrate.core.SimpleDBMigrate.get_all_migration_versions', return_value=[])
    def test_it_should_use_get_all_migrations_versions_method_to_get_all_migration_versions_up_to_a_version(self, get_all_migration_versions_mock):
        db_migrate = SimpleDBMigrate(self.config)
        db_migrate.get_all_migration_versions_up_to('20090214115200')
        self.assertEqual(1, get_all_migration_versions_mock.call_count)

    def test_it_should_check_if_migration_version_exists(self):
        db_migrate = SimpleDBMigrate(self.config)
        self.assertTrue(db_migrate.check_if_version_exists('20090214115100'))
        self.assertFalse(db_migrate.check_if_version_exists('19000101000000'))

    @patch('simple_db_migrate.core.SimpleDBMigrate.get_all_migration_versions', return_value=[])
    def test_it_should_use_get_all_migrations_versions_method_to_get_all_migration_versions_up_to_a_version(self, get_all_migration_versions_mock):
        db_migrate = SimpleDBMigrate(self.config)
        db_migrate.check_if_version_exists('20090214115100')
        self.assertEqual(1, get_all_migration_versions_mock.call_count)

    def test_it_should_not_inform_that_migration_version_exists_just_matching_the_beggining_of_version_number(self):
        db_migrate = SimpleDBMigrate(self.config)
        self.assertFalse(db_migrate.check_if_version_exists('2009'))

    def test_it_should_get_the_latest_version_available(self):
        db_migrate = SimpleDBMigrate(self.config)
        self.assertEqual('20090214115600', db_migrate.latest_version_available())

    @patch('simple_db_migrate.core.SimpleDBMigrate.get_all_migrations', return_value=[Mock(version='xpto')])
    def test_it_should_use_get_all_migrations_versions_method_to_get_the_latest_version_available(self, get_all_migrations_mock):
        db_migrate = SimpleDBMigrate(self.config)
        db_migrate.latest_version_available()
        self.assertEqual(1, get_all_migrations_mock.call_count)

    def test_it_should_get_migration_from_version_number(self):
        db_migrate = SimpleDBMigrate(self.config)
        migration = db_migrate.get_migration_from_version_number('20090214115100')
        self.assertEqual('20090214115100', migration.version)
        self.assertEqual('20090214115100_01_test_migration.migration', migration.file_name)

    def test_it_should_not_get_migration_from_invalid_version_number(self):
        db_migrate = SimpleDBMigrate(self.config)
        migration = db_migrate.get_migration_from_version_number('***invalid***')
        self.assertEqual(None, migration)

    @patch('simple_db_migrate.core.SimpleDBMigrate.get_all_migrations', return_value=[])
    def test_it_should_use_get_all_migrations_versions_method_to_get_migration_from_version_number(self, get_all_migrations_mock):
        db_migrate = SimpleDBMigrate(self.config)
        db_migrate.get_migration_from_version_number('20090214115100')
        self.assertEqual(1, get_all_migrations_mock.call_count)

class MigrationTest(BaseTest):
    def setUp(self):
        create_migration_file('20090214120600_example_file_name_test_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727104700_test_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727141400_test_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727141503_test_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727113900_empty_sql_up_test_migration.migration', sql_up='', sql_down='zzz')
        create_migration_file('20090727113900_empty_sql_down_test_migration.migration', sql_up='zzz', sql_down='')
        create_file('20090727114700_empty_file_test_migration.migration')
        create_file('20090727114700_without_sql_down_test_migration.migration', 'SQL_UP=""')
        create_file('20090727114700_without_sql_up_test_migration.migration', 'SQL_DOWN=""')

    def tearDown(self):
        delete_files('*test_migration.migration')

    def test_it_should_get_migration_version_from_file(self):
        migration = Migration('20090214120600_example_file_name_test_migration.migration')
        self.assertEqual('20090214120600', migration.version)

    def test_it_should_get_basic_properties_when_path_is_relative1(self):
        migration = Migration(file='20090727104700_test_migration.migration')
        self.assertEqual('20090727104700', migration.version)
        self.assertEqual('20090727104700_test_migration.migration', migration.file_name)
        self.assertEqual(os.path.abspath('./20090727104700_test_migration.migration'), migration.abspath)

    def test_it_should_get_basic_properties_when_path_is_relative2(self):
        migration = Migration(file='./20090727104700_test_migration.migration')
        self.assertEqual('20090727104700', migration.version)
        self.assertEqual('20090727104700_test_migration.migration', migration.file_name)
        self.assertEqual(os.path.abspath('./20090727104700_test_migration.migration'), migration.abspath)

    def test_it_should_get_basic_properties_when_path_is_relative3(self):
        here = os.path.dirname(os.path.relpath(__file__))
        migration = Migration(file='%s/../20090727104700_test_migration.migration' % here)
        self.assertEqual('20090727104700', migration.version)
        self.assertEqual('20090727104700_test_migration.migration', migration.file_name)
        self.assertEqual(os.path.abspath('./20090727104700_test_migration.migration'), migration.abspath)

    def test_it_should_get_basic_properties_when_path_is_absolute(self):
        migration = Migration(file=os.path.abspath('./20090727104700_test_migration.migration'))
        self.assertEqual('20090727104700', migration.version)
        self.assertEqual('20090727104700_test_migration.migration', migration.file_name)
        self.assertEqual(os.path.abspath('./20090727104700_test_migration.migration'), migration.abspath)

    def test_it_should_get_sql_up_and_down(self):
        migration = Migration(file='20090727104700_test_migration.migration')
        self.assertEqual(migration.sql_up, 'xxx')
        self.assertEqual(migration.sql_down, 'yyy')

    def test_it_should_get_sql_command_containing_unicode_characters(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='SQL_UP=u"some sql command"\nSQL_DOWN=u"other sql command"')
        migration = Migration(file_name)
        self.assertEqual(u"some sql command", migration.sql_up)
        self.assertEqual(u"other sql command", migration.sql_down)

    def test_it_should_get_sql_command_containing_unicode_characters_and_python_code(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='import os\nSQL_UP=u"some sql command %s" % os.path.abspath(\'.\')\nSQL_DOWN=u"other sql command %s" % os.path.abspath(\'.\')')
        migration = Migration(file_name)
        self.assertEqual(u"some sql command %s" % os.path.abspath('.'), migration.sql_up)
        self.assertEqual(u"other sql command %s" % os.path.abspath('.'), migration.sql_down)

    def test_it_should_get_sql_command_containing_unicode_characters_and_python_code_without_scope(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='SQL_UP=u"some sql command %s" % os.path.abspath(\'.\')\nSQL_DOWN=u"other sql command %s" % os.path.abspath(\'.\')')
        migration = Migration(file_name)
        self.assertEqual(u"some sql command %s" % os.path.abspath('.'), migration.sql_up)
        self.assertEqual(u"other sql command %s" % os.path.abspath('.'), migration.sql_down)

    def test_it_should_get_sql_command_containing_non_ascii_characters(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='SQL_UP=u"some sql command ç"\nSQL_DOWN=u"other sql command ã"')
        migration = Migration(file_name)
        self.assertEqual(u"some sql command ç", migration.sql_up)
        self.assertEqual(u"other sql command ã", migration.sql_down)

    def test_it_should_get_sql_command_containing_non_ascii_characters_and_python_code(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='import os\nSQL_UP=u"some sql command ç %s" % os.path.abspath(\'.\')\nSQL_DOWN=u"other sql command ã %s" % os.path.abspath(\'.\')')
        migration = Migration(file_name)
        self.assertEqual(u"some sql command ç %s" % os.path.abspath('.'), migration.sql_up)
        self.assertEqual(u"other sql command ã %s" % os.path.abspath('.'), migration.sql_down)

    def test_it_should_get_sql_command_containing_non_ascii_characters_and_python_code_without_scope(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='SQL_UP=u"some sql command ç %s" % os.path.abspath(\'.\')\nSQL_DOWN=u"other sql command ã %s" % os.path.abspath(\'.\')')
        migration = Migration(file_name)
        self.assertEqual(u"some sql command ç %s" % os.path.abspath('.'), migration.sql_up)
        self.assertEqual(u"other sql command ã %s" % os.path.abspath('.'), migration.sql_down)

    def test_it_should_get_sql_command_containing_non_ascii_characters_with_non_utf8_encoding(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='SQL_UP=u"some sql command ç"\nSQL_DOWN=u"other sql command ã"', encoding='iso8859-1')
        migration = Migration(file_name, script_encoding='iso8859-1')
        self.assertEqual(u"some sql command \xc3\xa7", migration.sql_up)
        self.assertEqual(u"other sql command \xc3\xa3", migration.sql_down)

    def test_it_should_get_sql_command_containing_non_ascii_characters_and_python_code_with_non_utf8_encoding(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='import os\nSQL_UP=u"some sql command ç %s" % os.path.abspath(\'.\')\nSQL_DOWN=u"other sql command ã %s" % os.path.abspath(\'.\')', encoding='iso8859-1')
        migration = Migration(file_name, script_encoding='iso8859-1')
        self.assertEqual(u"some sql command \xc3\xa7 %s" % os.path.abspath('.'), migration.sql_up)
        self.assertEqual(u"other sql command \xc3\xa3 %s" % os.path.abspath('.'), migration.sql_down)

    def test_it_should_get_sql_command_containing_non_ascii_characters_and_python_code_without_scope_with_non_utf8_encoding(self):
        file_name = '20090508155742_test_migration.migration'
        create_file(file_name, content='SQL_UP=u"some sql command ç %s" % os.path.abspath(\'.\')\nSQL_DOWN=u"other sql command ã %s" % os.path.abspath(\'.\')', encoding='iso8859-1')
        migration = Migration(file_name, script_encoding='iso8859-1')
        self.assertEqual(u"some sql command \xc3\xa7 %s" % os.path.abspath('.'), migration.sql_up)
        self.assertEqual(u"other sql command \xc3\xa3 %s" % os.path.abspath('.'), migration.sql_down)

    def test_it_should_raise_exception_when_migration_commands_are_empty(self):
        self.assertRaisesWithMessage(Exception, "migration command 'SQL_UP' is empty (%s)" % os.path.abspath('20090727113900_empty_sql_up_test_migration.migration'), Migration, '20090727113900_empty_sql_up_test_migration.migration')
        self.assertRaisesWithMessage(Exception, "migration command 'SQL_DOWN' is empty (%s)" % os.path.abspath('20090727113900_empty_sql_down_test_migration.migration'), Migration, '20090727113900_empty_sql_down_test_migration.migration')

    def test_it_should_raise_exception_when_migration_file_is_empty(self):
        self.assertRaisesWithMessage(Exception, "migration file is incorrect; it does not define 'SQL_UP' or 'SQL_DOWN' (%s)" % os.path.abspath('20090727114700_empty_file_test_migration.migration'), Migration, '20090727114700_empty_file_test_migration.migration')

    def test_it_should_raise_exception_when_migration_file_do_not_have_sql_up_constant(self):
        self.assertRaisesWithMessage(Exception, "migration file is incorrect; it does not define 'SQL_UP' or 'SQL_DOWN' (%s)" % os.path.abspath('20090727114700_without_sql_up_test_migration.migration'), Migration, '20090727114700_without_sql_up_test_migration.migration')

    def test_it_should_raise_exception_when_migration_file_do_not_have_sql_up_constant(self):
        self.assertRaisesWithMessage(Exception, "migration file is incorrect; it does not define 'SQL_UP' or 'SQL_DOWN' (%s)" % os.path.abspath('20090727114700_without_sql_down_test_migration.migration'), Migration, '20090727114700_without_sql_down_test_migration.migration')

    def test_it_should_compare_to_migration_versions_and_tell_which_is_newer(self):
        m1 = Migration('20090727104700_test_migration.migration')
        m2 = Migration('20090727141400_test_migration.migration')
        m3 = Migration('20090727141503_test_migration.migration')

        self.assertEqual(-1, m1.compare_to(m2))
        self.assertEqual(-1, m2.compare_to(m3))
        self.assertEqual(-1, m1.compare_to(m3))

        self.assertEqual(1, m2.compare_to(m1))
        self.assertEqual(1, m3.compare_to(m2))
        self.assertEqual(1, m3.compare_to(m1))

        self.assertEqual(0, m1.compare_to(m1))
        self.assertEqual(0, m2.compare_to(m2))
        self.assertEqual(0, m3.compare_to(m3))

    def test_it_should_raise_exception_when_file_does_not_exist(self):
        try:
            Migration('20090727104700_this_file_does_not_exist.migration')
        except Exception, e:
            self.assertEqual('migration file does not exist (20090727104700_this_file_does_not_exist.migration)', str(e))

    @patch('simple_db_migrate.core.Migration.is_file_name_valid', return_value=False)
    def test_it_should_raise_exception_when_file_name_is_invalid(self, is_file_name_valid_mock):
         self.assertRaisesWithMessage(Exception, 'invalid migration file name (simple-db-migrate.conf)', Migration, 'simple-db-migrate.conf')
         is_file_name_valid_mock.assert_called_with('simple-db-migrate.conf')

    def test_it_should_validate_if_filename_has_only_alphanumeric_chars_and_migration_extension(self):
        self.assertTrue(Migration.is_file_name_valid('20090214120600_valid_migration_file_name.migration'))
        self.assertFalse(Migration.is_file_name_valid('20090214120600_invalid_migration_file_name.migration~'))
        self.assertFalse(Migration.is_file_name_valid('simple-db-migrate.conf'))
        self.assertFalse(Migration.is_file_name_valid('abra.cadabra'))
        self.assertFalse(Migration.is_file_name_valid('randomrandomrandom.migration'))
        self.assertFalse(Migration.is_file_name_valid('21420101000000-wrong-separators.migration'))
        self.assertFalse(Migration.is_file_name_valid('2009021401_old_file_name_style.migration'))
        self.assertFalse(Migration.is_file_name_valid('20090214120600_good_name_bad_extension.foo'))
        self.assertFalse(Migration.is_file_name_valid('spamspamspamspamspaam'))

    @patch('simple_db_migrate.core.strftime', return_value='20120303194030')
    def test_it_should_create_migration_file(self, strftime_mock):
        self.assertFalse(os.path.exists('20120303194030_create_a_file_test_migration.migration'))
        Migration.create('create_a_file_test_migration', '.')
        self.assertTrue(os.path.exists('20120303194030_create_a_file_test_migration.migration'))

    @patch('simple_db_migrate.core.strftime', return_value='20120303194030')
    @patch('codecs.open', side_effect=IOError('error when writing'))
    def test_it_should_raise_exception_if_an_error_hapens_when_writing_the_file(self, open_mock, strftime_mock):
        try:
            Migration.create('test_migration')
            self.fail('it should not pass here')
        except Exception, e:
            self.assertEqual("could not create file ('./20120303194030_test_migration.migration')", str(e))

    @patch('simple_db_migrate.core.gmtime', return_value=(2012,03,03,19,40,30,0,0,0))
    def test_it_should_use_gmt_time_when_asked_to_use_utc(self, gmtime_mock):
        Migration.create('test_migration', utc_timestamp=True)
        gmtime_mock.assert_called_once()

    @patch('simple_db_migrate.core.localtime', return_value=(2012,03,03,19,40,30,0,0,0))
    def test_it_should_use_local_time_when_asked_to_not_use_utc(self, localtime_mock):
        Migration.create('test_migration', utc_timestamp=False)
        localtime_mock.assert_called_once()

    @patch('simple_db_migrate.core.Migration.is_file_name_valid', return_value=False)
    @patch('simple_db_migrate.core.strftime', return_value='20120303194030')
    def test_it_should_raise_exception_if_migration_has_a_invalid_name(self, strftime_mock, is_file_name_valid_mock):
        self.assertRaisesWithMessage(Exception, "invalid migration name ('#test_migration'); it should contain only letters, numbers and/or underscores", Migration.create, '#test_migration')
        is_file_name_valid_mock.assert_called_with('20120303194030_#test_migration.migration')

    def test_it_should_return_an_empty_string_if_sql_or_script_encoding_are_invalid(self):
        self.assertEqual('', Migration.ensure_sql_unicode('', ''))
        self.assertEqual('', Migration.ensure_sql_unicode('', "iso8859-1"))
        self.assertEqual('', Migration.ensure_sql_unicode('', None))
        self.assertEqual('', Migration.ensure_sql_unicode('', False))
        self.assertEqual('', Migration.ensure_sql_unicode('sql', ''))
        self.assertEqual('', Migration.ensure_sql_unicode(None, "iso8859-1"))
        self.assertEqual('', Migration.ensure_sql_unicode(False, "iso8859-1"))

    def test_it_should_convert_sql_to_unicode_from_script_encoding(self):
        self.assertEqual(u'sql in iso8859-1', Migration.ensure_sql_unicode('sql in iso8859-1'.encode("iso8859-1"), "iso8859-1"))

    def test_it_should_sort_a_migrations_list(self):
        migrations = []
        migrations.append(Migration('20090727141400_test_migration.migration'))
        migrations.append(Migration('20090214120600_example_file_name_test_migration.migration'))
        migrations.append(Migration('20090727141503_test_migration.migration'))
        migrations.append(Migration('20090727104700_test_migration.migration'))

        sorted_migrations = Migration.sort_migrations_list(migrations)
        self.assertEqual('20090214120600', sorted_migrations[0].version)
        self.assertEqual('20090727104700', sorted_migrations[1].version)
        self.assertEqual('20090727141400', sorted_migrations[2].version)
        self.assertEqual('20090727141503', sorted_migrations[3].version)

    def test_it_should_sort_a_migrations_list_in_rerverse_order(self):
        migrations = []
        migrations.append(Migration('20090727141400_test_migration.migration'))
        migrations.append(Migration('20090214120600_example_file_name_test_migration.migration'))
        migrations.append(Migration('20090727141503_test_migration.migration'))
        migrations.append(Migration('20090727104700_test_migration.migration'))

        sorted_migrations = Migration.sort_migrations_list(migrations, reverse=True)
        self.assertEqual('20090727141503', sorted_migrations[0].version)
        self.assertEqual('20090727141400', sorted_migrations[1].version)
        self.assertEqual('20090727104700', sorted_migrations[2].version)
        self.assertEqual('20090214120600', sorted_migrations[3].version)

if __name__ == '__main__':
    unittest.main()

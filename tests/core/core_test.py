# coding: utf-8
import codecs
import os
import stubs
import unittest

from simple_db_migrate.config import *
from simple_db_migrate.core import *

def create_file(file_name, content=None):
    f = codecs.open(file_name, 'w', 'utf-8')
    if content:
        f.write(content)
    f.close()
    return file_name

def create_migration_file(file_name, sql_up='', sql_down=''):
    f = codecs.open(file_name, 'w', 'utf-8')
    f.write('SQL_UP=u"%s"\nSQL_DOWN=u"%s"' % (sql_up, sql_down))
    f.close()
    return file_name
    
def create_config(host='localhost', username='root', password='', database='migration_example', migrations_dir='.'):
    config_file = '''
HOST = os.getenv('DB_HOST') or '%s'
USERNAME = os.getenv('DB_USERNAME') or '%s'
PASSWORD = os.getenv('DB_PASSWORD') or '%s'
DATABASE = os.getenv('DB_DATABASE') or '%s'
MIGRATIONS_DIR = os.getenv('MIGRATIONS_DIR') or '%s'
''' % (host, username, password, database, migrations_dir)
    f = open('test_config_file.conf', 'w')
    f.write(config_file)
    f.close()
    
    return FileConfig('test_config_file.conf')

class SimpleDBMigrateTest(unittest.TestCase):
    def setUp(self):
        self.config = create_config()
        self.test_migration_files = []
        
        # random migration_files
        self.test_migration_files.append(create_migration_file('20090214115100_example_migration_file1.migration', 'blah', 'blah'))
        self.test_migration_files.append(create_migration_file('20090214115200_example_migration_file2.migration', 'blah', 'blah'))
        self.test_migration_files.append(create_migration_file('20090214115300_example_migration_file3.migration', 'blah', 'blah'))
        self.test_migration_files.append(create_migration_file('20090214115400_example_migration_file4.migration', 'blah', 'blah'))
        self.test_migration_files.append(create_migration_file('20090214115500_example_migration_file5.migration', 'blah', 'blah'))
        self.test_migration_files.append(create_migration_file('20090214115600_example_migration_file6.migration', 'blah', 'blah'))
        
        # migration file with true SQL commands
        self.test_migration_files.append(create_migration_file('20090214120600_example_migration_file_with_commands.migration', 'create table test;', 'drop table test;'))
        
        # migration file with commands having unicode characters
        self.test_migration_files.append(create_file('20090508155742_example_migration_file_with_unicode_commands.migration', content=stubs.utf8_migration))
        
        # very very last schema version available
        self.test_migration_files.append(create_migration_file('21420101000000_example_migration_file.migration', 'blah', 'blah'))
        
        # migrations with bad file names
        self.test_migration_files_bad = []
        self.test_migration_files_bad.append(create_file('simple-db-migrate.conf'))
        self.test_migration_files_bad.append(create_file('abra.cadabra'))
        self.test_migration_files_bad.append(create_file('randomrandomrandom.migration'))
        self.test_migration_files_bad.append(create_file('21420101000000-wrong-separators.migration'))
        self.test_migration_files_bad.append(create_file('2009021401_old_file_name_style.migration'))
        self.test_migration_files_bad.append(create_file('20090214120600_good_name_bad_extension.foo'))
        self.test_migration_files_bad.append(create_file('spamspamspamspamspaam'))
            
    def tearDown(self):
        # remove all migrations
        db_migrate = SimpleDBMigrate(self.config)
        for migration in db_migrate.get_all_migrations():
            os.remove(migration.abspath)
        
        # remove bad files
        for each_file in self.test_migration_files_bad:
            os.remove(each_file)
        
        # remove temp config file
        os.remove('test_config_file.conf')

    def test_it_should_get_all_migrations_in_dir(self):
        db_migrate = SimpleDBMigrate(self.config)
        migrations = db_migrate.get_all_migrations()
        assert migrations is not None
        assert len(migrations) == len(self.test_migration_files)
        for migration in migrations:
            assert migration.file_name in self.test_migration_files
    
    def test_it_should_get_only_valid_migrations_in_dir(self):
        db_migrate = SimpleDBMigrate(self.config)
        migrations = db_migrate.get_all_migrations()
        for migration in migrations:
            assert migration.file_name in self.test_migration_files
            assert migration.file_name not in self.test_migration_files_bad
    
    def test_it_should_get_all_migration_versions_available(self):
        db_migrate = SimpleDBMigrate(self.config)
        migrations = db_migrate.get_all_migrations()
        expected_versions = []
        for migration in migrations:
            expected_versions.append(migration.version)
        
        all_versions = db_migrate.get_all_migration_versions()
        
        assert len(expected_versions) == len(all_versions)
        for version in all_versions:
            assert version in expected_versions
    
    def test_it_should_get_all_migration_versions_up_to_a_version(self):
        db_migrate = SimpleDBMigrate(self.config)
        migration_versions = db_migrate.get_all_migration_versions_up_to('20090214115200')
        assert len(migration_versions) == 1
        assert migration_versions[0] == '20090214115100'
    
    def test_it_should_check_if_schema_version_exists(self):
        db_migrate = SimpleDBMigrate(self.config)
        assert db_migrate.check_if_version_exists('20090214115100')
        assert not db_migrate.check_if_version_exists('19000101000000')
        
    def test_it_should_not_inform_that_schema_version_exists_just_matching_the_beggining_of_version_number(self):
        db_migrate = SimpleDBMigrate(self.config)
        assert not db_migrate.check_if_version_exists('2009')
    
    def test_it_should_get_the_latest_version_available(self):
        db_migrate = SimpleDBMigrate(self.config)
        assert db_migrate.latest_version_available() == '21420101000000'
    
    def test_it_should_get_migration_from_version_number(self):
        db_migrate = SimpleDBMigrate(self.config)
        migration = db_migrate.get_migration_from_version_number('20090214115100')
        assert migration.version == '20090214115100'
        assert migration.file_name == '20090214115100_example_migration_file1.migration'
    
    def test_it_should_not_get_migration_from_invalid_version_number(self):
        db_migrate = SimpleDBMigrate(self.config)
        migration = db_migrate.get_migration_from_version_number('***invalid***')
        assert migration is None
        
class MigrationTest(unittest.TestCase):
    def setUp(self):
        create_migration_file('20090214120600_example_migration_file_name.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727104700_sample_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727141400_sample_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727141503_sample_migration.migration', sql_up='xxx', sql_down='yyy')
        create_migration_file('20090727113900_sample_migration_empty_sql_up.migration', sql_up='', sql_down='zzz')
        create_migration_file('20090727113900_sample_migration_empty_sql_down.migration', sql_up='zzz', sql_down='')
        create_file('20090727114700_empty_file.migration')
    
    def tearDown(self):
        os.remove('20090214120600_example_migration_file_name.migration')
        os.remove('20090727104700_sample_migration.migration')
        os.remove('20090727141400_sample_migration.migration')
        os.remove('20090727141503_sample_migration.migration')
        os.remove('20090727113900_sample_migration_empty_sql_up.migration')
        os.remove('20090727113900_sample_migration_empty_sql_down.migration')
        os.remove('20090727114700_empty_file.migration')

    def test_it_should_get_migration_version_from_file(self):
        migration = Migration('20090214120600_example_migration_file_name.migration')
        assert migration.version == '20090214120600'

    def test_it_should_get_basic_properties_when_path_is_relative1(self):
        migration = Migration(file='20090727104700_sample_migration.migration')
        assert migration.version == '20090727104700'
        assert migration.file_name == '20090727104700_sample_migration.migration'
        assert migration.abspath == os.path.abspath('./20090727104700_sample_migration.migration')
    
    def test_it_should_get_basic_properties_when_path_is_relative2(self):
        migration = Migration(file='./20090727104700_sample_migration.migration')
        assert migration.version == '20090727104700'
        assert migration.file_name == '20090727104700_sample_migration.migration'
        assert migration.abspath == os.path.abspath('./20090727104700_sample_migration.migration')

    def test_it_should_get_basic_properties_when_path_is_relative3(self):
        here = os.path.dirname(os.path.relpath(__file__))
        migration = Migration(file='%s/../../20090727104700_sample_migration.migration' % here)
        assert migration.version == '20090727104700'
        assert migration.file_name == '20090727104700_sample_migration.migration'
        assert migration.abspath == os.path.abspath('./20090727104700_sample_migration.migration')

    def test_it_should_get_basic_properties_when_path_is_absolute(self):
        migration = Migration(file=os.path.abspath('./20090727104700_sample_migration.migration'))
        assert migration.version == '20090727104700'
        assert migration.file_name == '20090727104700_sample_migration.migration'
        assert migration.abspath == os.path.abspath('./20090727104700_sample_migration.migration')
    
    def test_it_should_get_sql_up_and_down(self):
        migration = Migration(file='20090727104700_sample_migration.migration')
        assert migration.sql_up == 'xxx'
        assert migration.sql_down == 'yyy'
    
    def test_it_should_get_sql_command_containing_unicode_characters(self):
        file_name = '20090508155742_example_migration_file_with_unicode_commands.migration'
        create_file(file_name, content=stubs.utf8_migration)
        migration = Migration(file_name)
        exec(stubs.utf8_migration)
        assert SQL_UP == migration.sql_up
        assert SQL_DOWN == migration.sql_down
    
    def test_it_should_raise_exception_when_file_does_not_exist(self):
        self.assertRaises(Exception, Migration, '20090727104700_this_file_does_not_exist.migration')
        
    def test_it_should_raise_exception_when_file_name_is_invalid(self):
        self.assertRaises(Exception, Migration, 'simple-db-migrate.conf')
        self.assertRaises(Exception, Migration, 'abra.cadabra')
        self.assertRaises(Exception, Migration, 'randomrandomrandom.migration')
        self.assertRaises(Exception, Migration, '21420101000000-wrong-separators.migration')
        self.assertRaises(Exception, Migration, '2009021401_old_file_name_style.migration')
        self.assertRaises(Exception, Migration, '20090214120600_good_name_bad_extension.foo')
        self.assertRaises(Exception, Migration, 'spamspamspamspamspaam')
        
    def test_it_should_not_validate_gedit_swap_files(self):
        invalid_file_name = '20090214120600_invalid_migration_file_name.migration~'
        assert not Migration.is_file_name_valid(invalid_file_name)

    def test_it_should_raise_exception_when_migration_commands_are_empty(self):
        self.assertRaises(Exception, Migration, '20090727113900_sample_migration_empty_sql_up.migration')
        self.assertRaises(Exception, Migration, '20090727113900_sample_migration_empty_sql_down.migration')
        
    def test_it_should_raise_exception_when_migration_file_is_empty(self):
        self.assertRaises(Exception, Migration, '20090727114700_empty_file.migration')
        
    def test_it_should_compare_to_migration_versions_and_tell_which_is_newer(self):
        m1 = Migration('20090727104700_sample_migration.migration')
        m2 = Migration('20090727141400_sample_migration.migration')
        m3 = Migration('20090727141503_sample_migration.migration')
        
        assert m1.compare_to(m2) == -1
        assert m2.compare_to(m3) == -1
        assert m1.compare_to(m3) == -1
        
        assert m2.compare_to(m1) == 1
        assert m3.compare_to(m2) == 1
        assert m3.compare_to(m1) == 1
        
        assert m1.compare_to(m1) == 0
        assert m2.compare_to(m2) == 0
        assert m3.compare_to(m3) == 0
    
    def test_it_should_create_migration_file(self):
        new_migration_file_name = Migration.create('create_a_migration_file', '.')
        assert Migration.is_file_name_valid(new_migration_file_name.replace('./', ''))
        
        migration = Migration(new_migration_file_name)
        assert os.path.exists(new_migration_file_name)

        os.remove(new_migration_file_name)

    def test_it_should_not_create_migration_file_with_bad_name(self):
        try:
            Migration.create('INVALID FILE NAME')
            self.fail('it should not pass here')
        except:
            pass

    def test_it_should_sort_a_migrations_list(self):
        migrations = []
        migrations.append(Migration('20090727141400_sample_migration.migration'))
        migrations.append(Migration('20090214120600_example_migration_file_name.migration'))
        migrations.append(Migration('20090727141503_sample_migration.migration'))
        migrations.append(Migration('20090727104700_sample_migration.migration'))
        
        sorted_migrations = Migration.sort_migrations_list(migrations)
        assert sorted_migrations[0].version == '20090214120600'
        assert sorted_migrations[1].version == '20090727104700'
        assert sorted_migrations[2].version == '20090727141400'
        assert sorted_migrations[3].version == '20090727141503'

    def test_it_should_sort_a_migrations_list_in_erverse_order(self):
        migrations = []
        migrations.append(Migration('20090727141400_sample_migration.migration'))
        migrations.append(Migration('20090214120600_example_migration_file_name.migration'))
        migrations.append(Migration('20090727141503_sample_migration.migration'))
        migrations.append(Migration('20090727104700_sample_migration.migration'))
        
        sorted_migrations = Migration.sort_migrations_list(migrations, reverse=True)
        assert sorted_migrations[0].version == '20090727141503'
        assert sorted_migrations[1].version == '20090727141400'
        assert sorted_migrations[2].version == '20090727104700'
        assert sorted_migrations[3].version == '20090214120600'

if __name__ == '__main__':
    unittest.main()

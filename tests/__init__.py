import glob
import os
import unittest
import codecs
from simple_db_migrate.config import *

def create_file(file_name, content=None, encoding='utf-8'):
    f = codecs.open(file_name, 'w', encoding)
    if content:
        f.write(content)
    f.close()
    return file_name

def create_migration_file(file_name, sql_up='', sql_down=''):
    create_file(file_name, 'SQL_UP=u"%s"\nSQL_DOWN=u"%s"' % (sql_up, sql_down))
    return file_name

def delete_files(pattern):
    filelist=glob.glob(pattern)
    for file in filelist:
        os.remove(file)

def create_config(host='localhost', username='root', password='', database='migration_example', migrations_dir='.', utc_timestamp=False, script_encoding='utf-8'):
    config_file = '''
HOST = os.getenv('DB_HOST') or '%s'
USERNAME = os.getenv('DB_USERNAME') or '%s'
PASSWORD = os.getenv('DB_PASSWORD') or '%s'
DATABASE = os.getenv('DB_DATABASE') or '%s'
MIGRATIONS_DIR = os.getenv('MIGRATIONS_DIR') or '%s'
UTC_TIMESTAMP = os.getenv("UTC_TIMESTAMP") or %s
DATABASE_SCRIPT_ENCODING = '%s'
''' % (host, username, password, database, migrations_dir, utc_timestamp, script_encoding)
    create_file('test_config_file.conf', config_file)
    return FileConfig('test_config_file.conf')

class BaseTest(unittest.TestCase):
    def tearDown(self):
        delete_files('*.log')
        delete_files('*test_migration.migration')
        delete_files('migrations/*test_migration.migration')
        if os.path.exists(os.path.abspath('migrations')):
            os.rmdir(os.path.abspath('migrations'))
        if os.path.exists(os.path.abspath('test_config_file.conf')):
            os.remove(os.path.abspath('test_config_file.conf'))

    def assertRaisesWithMessage(self, excClass, excMessage, callableObj, *args, **kwargs):
        raisedMessage = ''
        try:
            callableObj(*args, **kwargs)
        except excClass, e:
            raisedMessage = str(e)
            if excMessage == raisedMessage:
                return

        if hasattr(excClass,'__name__'): excName = excClass.__name__
        else: excName = str(excClass)
        raise self.failureException, "%s not raised with message '%s', the message was '%s'" % (excName, excMessage, raisedMessage)

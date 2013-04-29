import unittest
from simple_db_migrate.core.exceptions import MigrationException

class MigrationExceptionTest(unittest.TestCase):

    def test_it_should_use_default_message(self):
        exception = MigrationException()
        self.assertEqual('error executing migration', str(exception))

    def test_it_should_use_custom_message(self):
        exception = MigrationException('custom error message')
        self.assertEqual('custom error message', str(exception))

    def test_it_should_use_default_message_and_sql_command(self):
        exception = MigrationException(sql='sql command executed')
        self.assertEqual('error executing migration\n\n[ERROR DETAILS] SQL command was:\nsql command executed', str(exception))

    def test_it_should_use_custom_message_and_sql_command(self):
        exception = MigrationException(sql='sql command executed', msg='custom error message')
        self.assertEqual('custom error message\n\n[ERROR DETAILS] SQL command was:\nsql command executed', str(exception))

if __name__ == '__main__':
    unittest.main()

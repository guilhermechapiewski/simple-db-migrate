import unittest
import os
import logging
from datetime import datetime
from mock import patch, call, Mock
from simple_db_migrate.log import LOG
from tests import BaseTest, create_file, create_migration_file, delete_files, create_config

class LogTest(BaseTest):
    def tearDown(self):
        BaseTest.tearDown(self)
        delete_files('log_dir_test/path/subpath/*.log')
        if os.path.exists('log_dir_test/path/subpath'):
            os.rmdir('log_dir_test/path/subpath')
        if os.path.exists('log_dir_test/path'):
            os.rmdir('log_dir_test/path')
        if os.path.exists('log_dir_test'):
            os.rmdir('log_dir_test')

    def test_it_should_not_raise_error_if_log_dir_is_not_specified(self):
        try:
            log = LOG(None)
            log.debug('debug message')
            log.info('info message')
            log.error('error message')
            log.warn('warn message')
        except:
            self.fail("it should not get here")

    @patch('os.makedirs', side_effect=os.makedirs)
    def test_it_should_create_log_dir_if_does_not_exists(self, makedirs_mock):
        log = LOG('log_dir_test/path/subpath')
        expected_calls = [
            call('log_dir_test/path/subpath'),
            call('log_dir_test/path', 511),
            call('log_dir_test', 511)
        ]
        self.assertEqual(expected_calls, makedirs_mock.mock_calls)

    def test_it_should_create_a_logger(self):
        log = LOG('log_dir_test/path/subpath')
        self.assertTrue(isinstance(log.logger, logging.Logger))
        self.assertEqual(logging.DEBUG, log.logger.level)
        self.assertTrue(isinstance(log.logger.handlers[0], logging.FileHandler))
        self.assertEqual("%s/%s.log" %(os.path.abspath('log_dir_test/path/subpath'), datetime.now().strftime("%Y%m%d%H%M%S")), log.logger.handlers[0].baseFilename)
        self.assertTrue(isinstance(log.logger.handlers[0].formatter, logging.Formatter))
        self.assertEqual('%(message)s', log.logger.handlers[0].formatter._fmt)

    def test_it_should_use_logger_methods(self):
        log = LOG('log_dir_test/path/subpath')
        log.logger = Mock()
        log.debug('debug message')
        log.logger.debug.assert_called_with('debug message')
        log.info('info message')
        log.logger.info.assert_called_with('info message')
        log.error('error message')
        log.logger.error.assert_called_with('error message')
        log.warn('warn message')
        log.logger.warn.assert_called_with('warn message')


if __name__ == '__main__':
    unittest.main()

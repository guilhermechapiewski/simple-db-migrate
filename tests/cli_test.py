import unittest

from simple_db_migrate.cli import *

class CLITest(unittest.TestCase):

    def test_it_should_configure_all_options(self):
        cli = CLI()
        parser = cli.get_parser()
        
        self.assertTrue(parser.has_option("-h"))
        self.assertTrue(parser.has_option("--help"))
        
        self.assertTrue(parser.has_option("-c"))
        self.assertTrue(parser.has_option("--config"))
        
        self.assertTrue(parser.has_option("-m"))
        self.assertTrue(parser.has_option("--migration"))
        
        self.assertTrue(parser.has_option("-v"))
        self.assertTrue(parser.has_option("--version"))
        
        self.assertTrue(parser.has_option("--showsql"))
        self.assertTrue(parser.has_option("--showsqlonly"))
        
        self.assertTrue(parser.has_option("-n"))
        self.assertTrue(parser.has_option("--create"))
        self.assertTrue(parser.has_option("--new"))
        
        self.assertTrue(parser.has_option("--drop"))
        self.assertTrue(parser.has_option("--drop-database-first"))
        
        self.assertTrue(parser.has_option("--color"))
        
        self.assertTrue(parser.has_option("-l"))
        self.assertTrue(parser.has_option("--log-level"))
        
        self.assertTrue(parser.has_option("-p"))
        self.assertTrue(parser.has_option("--paused-mode"))

    def test_it_should_show_error_message_and_exit(self):
        cli = CLI()
        try:
            cli.error_and_exit("error test message, dont mind about it :)")
            self.fail("it should not get here")
        except:
            pass

    def test_it_should_show_info_message_and_exit(self):
        cli = CLI()
        try:
            cli.info_and_exit("info test message, dont mind about it :)")
            self.fail("it should not get here")
        except:
            pass

if __name__ == "__main__":
    unittest.main()

from test import *
from cli import *
import unittest

class CLITest(unittest.TestCase):

    def test_it_should_configure_all_options(self):
        cli = CLI()
        parser = cli.get_parser()
        
        self.assertTrue(parser.has_option("-h"))
        self.assertTrue(parser.has_option("--help"))
        
        self.assertTrue(parser.has_option("-c"))
        self.assertTrue(parser.has_option("--config"))
        
        self.assertTrue(parser.has_option("-d"))
        self.assertTrue(parser.has_option("--dir"))
        
        self.assertTrue(parser.has_option("-v"))
        self.assertTrue(parser.has_option("--version"))
        
        self.assertTrue(parser.has_option("--showsql"))
        
        self.assertTrue(parser.has_option("--create"))

if __name__ == "__main__":
    unittest.main()

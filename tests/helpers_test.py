from test import *
from helpers import *
from pmock import *
import unittest

class ListsTest(unittest.TestCase):
    
    def test_it_should_subtract_lists(self):
        a = ["a", "b", "c", "d", "e", "f"]
        b = ["a", "b", "c", "e"]
        result = Lists.subtract(a, b)
        
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0], "d")
        self.assertEquals(result[1], "f")

    def test_it_should_subtract_lists2(self):
        a = ["a", "b", "c", "e"]
        b = ["a", "b", "c", "d", "e", "f"]
        
        result = Lists.subtract(a, b)

        self.assertEquals(len(result), 0)

class UtilsTest(unittest.TestCase):
    
    def test_it_should_remove_config_file_name_from_path(self):
        self.assertEquals(Utils.get_path_without_config_file_name("../../simple-db-migrate.conf"), "../../")
        self.assertEquals(Utils.get_path_without_config_file_name("../../peanuts.conf"), "../../")
        self.assertEquals(Utils.get_path_without_config_file_name("/Users/gc/project/config_file.txt"), "/Users/gc/project/")
        self.assertEquals(Utils.get_path_without_config_file_name("../project/config_file.txt"), "../project/")
        self.assertEquals(Utils.get_path_without_config_file_name("../project/config_file"), "../project/")
        self.assertEquals(Utils.get_path_without_config_file_name("/etc/conf/project/config_file"), "/etc/conf/project/")
        self.assertEquals(Utils.get_path_without_config_file_name("sample.conf"), ".")

if __name__ == "__main__":
    unittest.main()
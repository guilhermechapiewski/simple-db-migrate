import unittest

from simple_db_migrate.helpers import *

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
    
    def test_it_should_count_chars_in_a_string(self):
        word = 'abbbcd;;;;;;;;;;;;;;'
        assert Utils.how_many(word, 'a') == 1
        assert Utils.how_many(word, 'b') == 3
        assert Utils.how_many(word, ';') == 14
        assert Utils.how_many(word, '%') == 0
    
    def test_it_should_raise_exception_when_char_to_match_is_not_valid(self):
        self.assertRaises(Exception, Utils.how_many, 'whatever', 'what')
        self.assertRaises(Exception, Utils.how_many, 'whatever', None)
        self.assertRaises(Exception, Utils.how_many, 'whatever', '')

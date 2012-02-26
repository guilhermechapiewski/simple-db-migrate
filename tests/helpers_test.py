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
        count = Utils.count_occurrences(word)
        self.assertEqual( 1, count.get('a', 0))
        self.assertEqual( 3, count.get('b', 0))
        self.assertEqual(14, count.get(';', 0))
        self.assertEqual( 0, count.get('%', 0))

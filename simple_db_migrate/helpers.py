class Lists(object):

    @classmethod
    def subtract(self, list_a, list_b):
        return [l for l in list_a if l not in list_b]

class Utils(object):

    @classmethod
    def count_occurrences(self, string):
        count = {}
        for char in string:
            count[char] = count.get(char, 0) + 1
        return count

class Lists(object):

    @classmethod
    def subtract(self, list_a, list_b):
        return [l for l in list_a if l not in list_b]

class Utils(object):

    @classmethod
    def how_many(self, string, match):
        if not match or len(match) != 1:
            raise Exception("match should be a char")
        count = {}
        for char in string:
            count[char] = count.get(char, 0) + 1
        return count.get(match, 0)
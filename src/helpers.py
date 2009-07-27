import re

class Lists(object):
    
    @classmethod
    def subtract(self, list_a, list_b):
        return [l for l in list_a if l not in list_b]
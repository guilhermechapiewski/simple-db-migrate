import re

class Lists(object):
    
    @classmethod
    def subtract(self, list_a, list_b):
        return [l for l in list_a if l not in list_b]
        
class Utils(object):
    
    @classmethod
    def get_path_without_config_file_name(self, path):
        info = re.match(r"^(.*/).*$", path, re.S)
        if info is None or info == "":
            return "."
        return info.group(1)
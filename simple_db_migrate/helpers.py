import os
import sys
import tempfile
import re

class Lists(object):

    @staticmethod
    def subtract(list_a, list_b):
        return [l for l in list_a if l not in list_b]

class Utils(object):
    
    @staticmethod
    def normalize_sql(string):
        # Basically just remove all the content between quotes so we can see if it's valid-ish later
        
        enclosed_quotes = re.compile("'.*?[^\\\\]'", re.S | re.U)
        
        sql = enclosed_quotes.sub("", string.strip())
        
        return sql
    
    @staticmethod
    def count_occurrences(string):
        count = {}
        for char in string:
            count[char] = count.get(char, 0) + 1
        return count

    @staticmethod
    def get_variables_from_file(full_filename, file_encoding='utf-8'):
        path, filename = os.path.split(full_filename)
        temp_abspath = None

        global_dict = globals().copy()

        try:
            # add settings dir from path
            sys.path.insert(0, path)

            execfile(full_filename, global_dict, global_dict)
        except IOError:
            raise Exception("%s: file not found" % full_filename)
        except Exception, e:
            try:
                f = open(full_filename, "rU")
                content = f.read()
                f.close()

                temp_abspath = "%s/%s" %(tempfile.gettempdir().rstrip('/'), filename)
                f = open(temp_abspath, "w")
                f.write('#-*- coding:%s -*-\n%s' % (file_encoding, content))
                f.close()

                execfile(temp_abspath, global_dict, global_dict)
            except Exception, e:
                raise Exception("error interpreting config file '%s': %s" % (filename, str(e)))
        finally:
            #erase temp and compiled files
            if temp_abspath and os.path.isfile(temp_abspath):
                os.remove(temp_abspath)

            # remove settings dir from path
            if path in sys.path:
                sys.path.remove(path)


        local_dict = {}
        globals_keys = globals().keys()
        for key in global_dict:
            if key not in globals_keys:
                local_dict[key] = global_dict[key]

        return local_dict
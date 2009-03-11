import sys

class Log(object):

    def __print(self, level, msg):
        print "[%s] %s\n" % (level, msg)

    def error_and_exit(self, msg):
        self.__print("ERROR", msg)
        sys.exit(1)
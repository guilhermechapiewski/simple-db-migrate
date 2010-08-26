#!/usr/bin/env python
# encoding: utf-8

"""
Module with helper methods to console display and
manipulation.
"""

import sys
from datetime import datetime

class ConsoleChar(object):
    '''
    Helper class that provides access to the Console
    chars available to db-migrate.
    '''
    pink = "\033[35m"
    blue = "\033[34m"
    cyan = "\033[36m"
    green = "\033[32m"
    yellow = "\033[33m"
    red = "\033[31m"
    end = "\033[0m"
    
    @classmethod
    def colored_message(cls, message, color):
        '''returns the message formatted with the given color'''
        return "%s%s%s" % (color, message, cls.end)

class Actions(object):
    '''
    Available actions to the console to take.
    '''
    @classmethod
    def error_and_exit(cls, msg):
        '''Displays an error message and exits the application'''
        Actions.log("[ERROR] %s\n" % msg, ConsoleChar.red)
        sys.exit(1)

    @classmethod
    def info_and_exit(cls, msg):
        '''Displays a success message and exits the application'''
        Actions.log("%s\n" % msg, ConsoleChar.blue)
        sys.exit(0)

    @classmethod
    def log(cls, message, color=ConsoleChar.cyan):
        '''
        Displays a colored message (defaults to cyan)
        in the stdout stream.
        '''
        time = datetime.now().strftime('%H:%M:%S') 
        formatted_msg = "[%s] %s" % (time, message)
        colored_msg = ConsoleChar.colored_message(formatted_msg, color)
        sys.stdout.write(colored_msg)

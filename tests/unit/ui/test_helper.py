#!/usr/bin/env python
# encoding: utf-8

from datetime import datetime

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify

from db_migrate import Version
import db_migrate.ui.helper as helper
from db_migrate.ui.helper import ConsoleChar, Actions

def test_helper_colors_pink():
    assert ConsoleChar.pink == "\033[35m"

def test_helper_colors_blue():
    assert ConsoleChar.blue == "\033[34m"

def test_helper_colors_cyan():
    assert ConsoleChar.cyan == "\033[36m"

def test_helper_colors_green():
    assert ConsoleChar.green == "\033[32m"

def test_helper_colors_yellow():
    assert ConsoleChar.yellow == "\033[33m"

def test_helper_colors_red():
    assert ConsoleChar.red == "\033[31m"

def test_helper_end_of_line():
    assert ConsoleChar.end == "\033[0m"

@with_fakes
@with_patched_object(helper, 'sys', Fake('sys'))
def test_actions_log_with_cyan_as_default_color():
    clear_expectations()

    time = datetime.now().strftime('%H:%M:%S') 
    helper.sys.stdout = Fake('stdout')
    helper.sys.stdout.expects('write').with_args('\x1b[36m[%s] message\x1b[0m' % time)

    Actions.log("message")

@with_fakes
@with_patched_object(helper, 'sys', Fake('sys'))
@with_patched_object(Actions, 'log', Fake(callable=True))
def test_info_and_exit():
    clear_expectations()

    Actions.log.with_args('message\n', ConsoleChar.blue)
    helper.sys.expects('exit').with_args(0).times_called(1)

    Actions.info_and_exit('message')

@with_fakes
@with_patched_object(helper, 'sys', Fake('sys'))
@with_patched_object(Actions, 'log', Fake(callable=True))
def test_error_and_exit():
    clear_expectations()

    Actions.log.with_args('[ERROR] message\n', ConsoleChar.red)
    helper.sys.expects('exit').with_args(1).times_called(1)

    Actions.error_and_exit('message')

    
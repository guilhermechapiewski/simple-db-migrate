#!/usr/bin/env python
# encoding: utf-8

import sys

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify
from fudge.inspector import arg

import db_migrate.ui.console as cons
from db_migrate.ui.console import Console

@with_fakes
@with_patched_object(Console, 'parse_arguments', Fake(expect_call=True))
@with_patched_object(Console, 'assert_and_get_action', Fake(callable=True))
def test_console_run_calls_parse_arguments():
    clear_expectations()

    args = ['test']

    Console.parse_arguments.with_args(args)
    Console.parse_arguments.next_call().with_args(sys.argv[1:])

    called = Fake('called')
    called.called = False
    def do_something(arguments, options):
        called.called = True

    Console.assert_and_get_action.with_args('test').returns(do_something)
    Console.assert_and_get_action.next_call().with_args('migrate').returns(do_something)

    console = Console()

    console.arguments = ['test']
    console.run(arguments=args)

    assert called.called
    called.called = False

    console.arguments = []
    console.run()

    assert called.called

@with_fakes
@with_patched_object(cons, 'OptionParser', Fake(callable=True))
def test_console_parse_arguments_creates_proper_optparse():
    clear_expectations()

    args = []

    fake_parser = Fake('parser')
    cons.OptionParser.returns(fake_parser)

    fake_parser.expects('add_option').times_called(10)
    fake_parser.expects('parse_args').with_args(args).times_called(1).returns(([], []))

    console = Console()

    console.parse_arguments(args)

@with_fakes
@with_patched_object(cons, 'console_actions', Fake('console_actions'))
@with_patched_object(cons, 'Actions', Fake('Actions'))
def test_assert_and_get_action_exits_with_error_if_action_not_found():
    clear_expectations()

    msg = 'The specified action of migrate was not found. Available actions are: '
    cons.console_actions.ACTIONS = {}
    cons.Actions.expects('error_and_exit').with_args(msg).raises(ValueError('Exit'))
    
    console = Console()

    try:
        console.assert_and_get_action('migrate')
    except ValueError, err:
        assert str(err) == "Exit"
        return
    assert False, "Should not have gotten this far"

@with_fakes
@with_patched_object(cons, 'console_actions', Fake('console_actions'))
def test_assert_and_get_action_returns_action_callable():
    clear_expectations()

    msg = 'The specified action of migrate was not found. Available actions are: '
    cons.console_actions.ACTIONS = {'migrate':'whatever'}
    
    console = Console()

    assert console.assert_and_get_action('migrate') == "whatever"

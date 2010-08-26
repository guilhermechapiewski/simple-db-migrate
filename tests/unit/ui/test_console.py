#!/usr/bin/env python
# encoding: utf-8

import sys

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify
from fudge.inspector import arg

import db_migrate.ui.console as cons
from db_migrate.ui.console import Console

@with_fakes
@with_patched_object(Console, 
                     'parse_arguments',
                     Fake(expect_call=True)
                    )
def test_console_run_calls_parse_arguments():
    clear_expectations()

    args = []

    Console.parse_arguments.with_args(args)
    Console.parse_arguments.next_call().with_args(sys.argv[1:])

    console = Console()

    console.run(arguments=args)

    console.run()

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


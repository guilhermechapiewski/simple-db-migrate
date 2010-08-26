#!/usr/bin/env python
# encoding: utf-8

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify

import db_migrate.ui.console_actions as actions

def clear():
    actions.ACTIONS = {}

def test_actions_auto_register_themselves():
    clear()

    @actions.action('some_action')
    def some_action():
        pass

    assert 'some_action' in actions.ACTIONS
    assert actions.ACTIONS['some_action'] == some_action

def test_registering_action_twice_fails():
    clear()

    @actions.action('some_action')
    def some_action():
        pass

    try:
        @actions.action('some_action')
        def other_action():
            pass
    except RuntimeError, err:
        assert str(err) == 'The action with command some_action is already registered.'
        return
    assert False, "Shouldn't have gotten this far."
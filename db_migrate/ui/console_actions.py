#!/usr/bin/env python
# encoding: utf-8

ACTIONS = {}

def action(command):
    def function(fn):
        if command in ACTIONS:
            raise RuntimeError('The action with command %s is already registered.' % command)
        ACTIONS[command] = fn

        return fn
    return function

@action('migrate')
def auto_migrate(arguments, options):
    pass
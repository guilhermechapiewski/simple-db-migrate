#!/usr/bin/env python
# encoding: utf-8

from datetime import datetime

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify

import db_migrate

def test_db_migrate_version_is_there():
    assert hasattr(db_migrate, 'Version')
    assert hasattr(db_migrate, '__version__')
    assert hasattr(db_migrate, 'version')

    assert db_migrate.Version == db_migrate.version == db_migrate.__version__

@with_fakes
@with_patched_object(db_migrate, 'Console', Fake(callable=True))
def test_run_calls_console_run():
    clear_expectations()

    console_fake = Fake('console')
    db_migrate.Console.returns(console_fake)
    console_fake.expects('run')

    db_migrate.run()

#!/usr/bin/env python
# encoding: utf-8

from os.path import abspath, dirname, join

from db_migrate.domain.migrations import Migration

test_path = abspath(dirname(__file__))

def test_migration_can_load_a_file_with_proper_data():
    migration_file = '20101213141516_test_migration_1.migration'
    migration = Migration(join(test_path, migration_file))
    migration.load()

    assert migration.version == "20101213141516"
    assert migration.title == "test_migration_1"

    assert len(migration.up_statements) == 3
    assert migration.up_statements[0] == "some test command"
    assert migration.up_statements[1] == "another test command"
    assert migration.up_statements[2] == "yet one more test command"

    assert len(migration.down_statements) == 3
    assert migration.down_statements[0] == "yet one more test command"
    assert migration.down_statements[1] == "another test command"
    assert migration.down_statements[2] == "some test command"
    
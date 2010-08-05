#!/usr/bin/env python
# encoding: utf-8

from db_migrate.domain.migrations import Migration

def test_can_create_migration_model():
    path = "/tmp/20101010101010_doing_some_db_changes.migration"
    migr = Migration(filepath=path)

    assert migr
    assert migr.filepath == path
    assert migr.filename == "20101010101010_doing_some_db_changes.migration"
    assert migr.version == "20101010101010"
    assert migr.title == "doing_some_db_changes"
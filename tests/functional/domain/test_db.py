#!/usr/bin/env python
# encoding: utf-8

from db_migrate.config import Config
from db_migrate.domain.db import Db

TEST_DB_CONFIG = Config.static(
    dbtype='mysql',
    user='root',
    password='',
    host='localhost',
    port=None,
    db='db_migrate_test_database'
)
NEW_DB_CONFIG = Config.static(
    dbtype='mysql',
    user='root',
    password='',
    host='localhost',
    port=None,
    db='db_migrate_test_database_2'
)

def test_connect_to_some_database():
    db = Db(config=TEST_DB_CONFIG)

    db.connect()

    assert db.connection
    assert db.connection.scalar('select 1 from dual') == 1L

def test_connect_to_main_database():
    db = Db(config=TEST_DB_CONFIG)

    db.connect(to_main_database=True)

    assert db.connection
    assert db.connection.scalar('select 1 from dual') == 1L

def test_can_close_connection():
    db = Db(config=TEST_DB_CONFIG)

    db.connect()

    assert db.connection

    db.close()
    assert not db.connection

def test_can_execute_command_in_some_database():
    db = Db(config=TEST_DB_CONFIG)

    result = db.execute('select 1 from dual')

    assert result.scalar() == 1L

def test_can_execute_command_in_main_database():
    db = Db(config=TEST_DB_CONFIG)

    result = db.execute('select 1 from dual', to_main_database=True)

    assert result.scalar() == 1L

def test_can_scalar_query_command_in_some_database():
    db = Db(config=TEST_DB_CONFIG)

    result = db.query_scalar('select 1 from dual')

    assert result == 1L

def test_can_scalar_query_command_in_main_database():
    db = Db(config=TEST_DB_CONFIG)

    result = db.query_scalar('select 1 from dual', to_main_database=True)

    assert result == 1L

def test_can_create_database():
    db = Db(config=NEW_DB_CONFIG)

    db.create_database()

    new_db = Db(config=TEST_DB_CONFIG)
    
    results = new_db.execute('show databases', to_main_database=True)
    dbs = [result[0] for result in results.fetchall()]
    assert 'db_migrate_test_database_2' in dbs

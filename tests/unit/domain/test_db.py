#!/usr/bin/env python
# encoding: utf-8

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify

from db_migrate.domain.db import Db
import db_migrate.domain.db as db_module
from db_migrate.domain.errors import *

def fake_config(dbtype='postgre', db='myDb', host='myHost', 
                port=1111, user='myUser', password='myPass'):
    config = Fake('config').has_attr(
        dbtype=dbtype,
        db=db,
        host=host,
        port=port,
        user=user,
        password=password
    )

    return config

db_types = {
                'postgre': 'postgresql://myUser:myPass@myHost/myDb',
                'mysql': 'mysql://myUser:myPass@myHost/myDb',
                'oracle': 'oracle://myUser:myPass@myHost:1111/myDb',
                'mssql': 'mssql://myDb',
                'sqlite': 'sqlite:///myHost',
                'in-memory-sqlite': 'sqlite://'
           }

main_db_types = {
                'postgre': 'postgresql://myUser:myPass@myHost/',
                'mysql': 'mysql://myUser:myPass@myHost/mysql',
                'oracle': 'oracle://myUser:myPass@myHost:1111/',
                'mssql': 'mssql://',
                'sqlite': 'sqlite:///myHost',
                'in-memory-sqlite': 'sqlite://'
           }

def test_db_gets_proper_conn_str():
   for db_type in db_types.keys():
      yield assert_db_gets_proper_conn_str, db_type

def test_db_gets_proper_main_conn_str():
   for db_type in db_types.keys():
      yield assert_db_gets_proper_main_conn_str, db_type

@with_fakes
def assert_db_gets_proper_conn_str(dbtype):
    clear_expectations()

    config = fake_config(dbtype=dbtype)

    db = Db(config)
    
    expected_conn_str = db_types[dbtype]
    assert db.connection_string == expected_conn_str

@with_fakes
def assert_db_gets_proper_main_conn_str(dbtype):
    clear_expectations()

    config = fake_config(dbtype=dbtype)

    db = Db(config)
    
    expected_conn_str = main_db_types[dbtype]
    assert db.main_database_connection_string == expected_conn_str

@with_fakes
@with_patched_object(db_module, 'create_engine', Fake(callable=True))
def test_connect_creates_engine_and_connects():
    clear_expectations()

    config = fake_config()

    db = Db(config)

    fake_engine = Fake('engine')
    fake_connection = Fake('connection')

    db_module.create_engine \
             .with_args(db.connection_string) \
             .returns(fake_engine)

    fake_engine.expects('connect').returns(fake_connection)

    db.connect()
    
    assert db.connection == fake_connection

@with_fakes
def test_close_exits_gracefully_when_no_connection_been_made():
    clear_expectations()

    config = fake_config()

    db = Db(config)

    assert not db.connection

    db.close()

    assert not db.connection

@with_fakes
@with_patched_object(Db, 'execute', Fake(callable=True))
def test_create_db():
    clear_expectations()

    config = fake_config()

    db = Db(config)

    Db.execute.with_args('CREATE DATABASE IF NOT EXISTS myDb', to_main_database=True)

    db.create_database()

@with_fakes
@with_patched_object(Db, 'connect', Fake(callable=True))
def test_execute_calls_connect_if_no_connection_done():
    clear_expectations()

    config = fake_config()

    db = Db(config)

    fake_connection = Fake('connection')

    fake_connection.expects('execute').with_args('select 1 from dual')

    def set_connection(db, connection):
        def sets_attr(to_main_database):
            setattr(db, 'connection', fake_connection)
        return sets_attr

    Db.connect.calls(set_connection(db, fake_connection))

    db.execute('select 1 from dual')

@with_fakes
def test_execute_does_not_call_connect_if_connection_done():
    clear_expectations()

    config = fake_config()

    db = Db(config)
    db.connection = Fake('connection')

    db.connection.expects('execute').with_args('select 1 from dual')

    db.execute('select 1 from dual')

@with_fakes
@with_patched_object(Db, 'execute', Fake(callable=True))
def test_query_scalar():
    clear_expectations()

    config = fake_config()

    db = Db(config)

    Db.execute.returns([[1]])

    result = db.query_scalar('select 1 from dual')

    assert result == 1

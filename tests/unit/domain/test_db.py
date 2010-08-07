#!/usr/bin/env python
# encoding: utf-8

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify

from db_migrate.domain.db import Db
from db_migrate.domain.errors import *

db_types = {
            'postgre': 'postgresql://myUser:myPass@myHost/myDb',
            'mysql': 'mysql://myUser:myPass@myHost/myDb',
            'oracle': 'oracle://myUser:myPass@myHost:1111/myDb',
            'mssql': 'mssql://myDb',
            'sqlite': 'sqlite:///myHost',
            'in-memory-sqlite': 'sqlite://'
           }

def test_db_gets_proper_conn_str():
   for db_type in db_types.keys():
      yield assert_db_gets_proper_conn_str, db_type

@with_fakes
def assert_db_gets_proper_conn_str(dbtype):
    clear_expectations()

    config = Fake('config').has_attr(
        dbtype=dbtype,
        db='myDb',
        host='myHost',
        port=1111,
        user='myUser',
        password='myPass'
    )

    db = Db(config)
    
    expected_conn_str = db_types[dbtype]
    assert db.connection_string == expected_conn_str


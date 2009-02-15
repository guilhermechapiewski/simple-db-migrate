from _test_env import *
from mysql import *
from pmock import *
import os
import unittest

class MySQLTest(unittest.TestCase):

    def setUp(self):
        f = open("test.conf", "w")
        f.write("HOST = 'localhost'\nUSERNAME = 'root'\nPASSWORD = ''\nDATABASE = 'migration_test'")
        f.close()
        
    def tearDown(self):
        os.remove("test.conf")

    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        cursor_mock = Mock()
        cursor_mock.expects(once()).method("execute").execute(eq("select count(*) from __db_version__;"))
        cursor_mock.expects(once()).method("fetchone").will(return_value("0"))
    
        db_mock = Mock()
        db_mock.expects(once()).method("query").query(eq("create database if not exists migration_test;"))
        db_mock.expects(once()).method("query").query(eq("create table if not exists __db_version__ ( version int(11) NOT NULL default 0 );"))
        db_mock.expects(once()).method("query").query(eq("insert into __db_version__ values (0);"))
        db_mock.expects(once()).method("cursor").will(return_value(cursor_mock))
        db_mock.expects(at_least_once()).method("close")
        
        mysql_driver_mock = Mock()
        mysql_driver_mock.expects(at_least_once()).method("connect").will(return_value(db_mock))
        
        mysql = MySQL("test.conf", mysql_driver=mysql_driver_mock)

if __name__ == "__main__":
    unittest.main()

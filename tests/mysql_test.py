from test import *
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
    
    def __create_init_expectations(self, mysql_driver_mock, db_mock, cursor_mock):
        mysql_driver_mock.expects(at_least_once()).method("connect").will(return_value(db_mock))
        
        db_mock.expects(once()).method("query")
        db_mock.expects(once()).method("close")
        db_mock.expects(once()).method("query")
        db_mock.expects(once()).method("close")
        
        db_mock.expects(at_least_once()).method("cursor").will(return_value(cursor_mock))
        cursor_mock.expects(once()).method("execute")
        cursor_mock.expects(once()).method("fetchone").will(return_value("0"))
        db_mock.expects(once()).method("close")
        
    
    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        mysql_driver_mock.expects(at_least_once()).method("connect").will(return_value(db_mock))
        
        db_mock.expects(at_least_once()).method("close")
        db_mock.expects(once()).method("query").query(eq("create database if not exists migration_test;"))
        db_mock.expects(once()).method("query").query(eq("create table if not exists __db_version__ ( version varchar(20) NOT NULL default \"0\" );"))
        db_mock.expects(once()).method("cursor").will(return_value(cursor_mock))
        
        cursor_mock.expects(once()).method("execute").execute(eq("select count(*) from __db_version__;"))
        cursor_mock.expects(once()).method("fetchone").will(return_value("0"))
    
        db_mock.expects(once()).method("query").query(eq("insert into __db_version__ values (\"0\");"))
        
        mysql = MySQL("test.conf", mysql_driver_mock)
        
    def test_it_should_execute_changes_and_update_schema_version(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        self.__create_init_expectations(mysql_driver_mock, db_mock, cursor_mock)
                
        db_mock.expects(once()).method("query").query(eq("create table spam();"))
        db_mock.expects(once()).method("close")
        db_mock.expects(once()).method("query").query(eq("insert into __db_version__ (version) values (\"20090212112104\");"))
        db_mock.expects(once()).method("close")
        
        mysql = MySQL("test.conf", mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104")
        
    def test_it_should_get_current_schema_version(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        self.__create_init_expectations(mysql_driver_mock, db_mock, cursor_mock)
        
        cursor_mock.expects(once()).method("execute").execute(eq("select version from __db_version__ order by version desc limit 0,1;"))
        cursor_mock.expects(once()).method("fetchone").will(return_value("0"))
        db_mock.expects(once()).method("close")
        
        mysql = MySQL("test.conf", mysql_driver_mock)
        self.assertEquals("0", mysql.get_current_schema_version())

if __name__ == "__main__":
    unittest.main()

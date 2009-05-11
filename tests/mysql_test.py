from test import *
from core import *
from mysql import *
from pmock import *
import os
import unittest

class MySQLTest(unittest.TestCase):

    def setUp(self):
        config_file = """
HOST = os.getenv("DB_HOST") or "localhost"
USERNAME = os.getenv("DB_USERNAME") or "root"
PASSWORD = os.getenv("DB_PASSWORD") or ""
DATABASE = os.getenv("DB_DATABASE") or "migration_test"
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR") or "."
"""
        f = open("test.conf", "w")
        f.write(config_file)
        f.close()

        self.__config = Config("test.conf")
        self.__config.put("drop_db_first", False)
        
    def tearDown(self):
        os.remove("test.conf")
    
    def __mock_db_init(self, mysql_driver_mock, db_mock, cursor_mock):
        mysql_driver_mock.expects(at_least_once()).method("connect").will(return_value(db_mock))
        db_mock.expects(at_least_once()).method("set_character_set")
        db_mock.expects(at_least_once()).method("select_db")
        
        # create db if not exists
        db_mock.expects(once()).method("query").query(eq("create database if not exists migration_test;"))
        db_mock.expects(once()).method("close")
        
        # create version table if not exists
        create_version_table = "create table if not exists __db_version__ ( version varchar(20) NOT NULL default \"0\" );"
        self.__mock_db_execute(db_mock, cursor_mock, create_version_table)
        
        # check if exists any version
        db_mock.expects(once()).method("cursor").will(return_value(cursor_mock))
        cursor_mock.expects(once()).method("execute").execute(eq("select count(*) from __db_version__;"))
        cursor_mock.expects(once()).method("fetchone").will(return_value("0"))
        db_mock.expects(once()).method("close")
    
    def __mock_db_execute(self, db_mock, cursor_mock, query):
        # mock a call to __execute
        db_mock.expects(once()).method("cursor").will(return_value(cursor_mock))
        
        sql_statements = query.split(";")
        sql_statements = [s.strip() for s in sql_statements if s.strip() != ""]
        for statement in sql_statements:
            cursor_mock.expects(once()).method("execute").execute(eq(statement))
            
        cursor_mock.expects(once()).method("close")
        db_mock.expects(once()).method("commit")
        db_mock.expects(once()).method("close")
    
    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        mysql = MySQL(self.__config, mysql_driver_mock)
    
    def test_it_should_drop_database_on_init_if_its_asked(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        db_mock.expects(once()).method("query").query(eq("drop database if exists migration_test;"))
        db_mock.expects(once()).method("close")

        mysql = MySQL(self.__config, mysql_driver=mysql_driver_mock)
    
    def test_it_should_execute_migration_up_and_update_schema_version(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()

        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        self.__mock_db_execute(db_mock, cursor_mock, "create table spam();")
        self.__mock_db_execute(db_mock, cursor_mock, "insert into __db_version__ (version) values (\"20090212112104\");")

        mysql = MySQL(self.__config, mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104")
        
    def test_it_should_execute_migration_down_and_update_schema_version(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()

        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        self.__mock_db_execute(db_mock, cursor_mock, "create table spam();")
        self.__mock_db_execute(db_mock, cursor_mock, "delete from __db_version__ where version >= \"20090212112104\";")

        mysql = MySQL(self.__config, mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104", False)
    
    def test_it_should_get_current_schema_version(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        db_mock.expects(once()).method("cursor").will(return_value(cursor_mock))
        cursor_mock.expects(once()).method("execute").execute(eq("select version from __db_version__ order by version desc limit 0,1;"))
        cursor_mock.expects(once()).method("fetchone").will(return_value("0"))
        db_mock.expects(once()).method("close")
        
        mysql = MySQL(self.__config, mysql_driver_mock)
        self.assertEquals("0", mysql.get_current_schema_version())
    
    def test_it_should_get_all_schema_versions(self):
        mysql_driver_mock = Mock()
        db_mock = Mock()
        cursor_mock = Mock()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        expected_versions = []
        expected_versions.append("0")
        expected_versions.append("20090211120001")
        expected_versions.append("20090211120002")
        expected_versions.append("20090211120003")
        
        db_mock.expects(at_least_once()).method("cursor").will(return_value(cursor_mock))
        cursor_mock.expects(once()).method("execute").execute(eq("select version from __db_version__ order by version;"))
        cursor_mock.expects(once()).method("fetchall").will(return_value(tuple(zip(expected_versions))))
        db_mock.expects(once()).method("close")
        
        mysql = MySQL(self.__config, mysql_driver_mock)
        
        schema_versions = mysql.get_all_schema_versions()
        self.assertEquals(len(expected_versions), len(schema_versions))
        for version in schema_versions:
            self.assertTrue(version in expected_versions)
    
if __name__ == "__main__":
    unittest.main()

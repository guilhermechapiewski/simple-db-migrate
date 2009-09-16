from test import *
from config import *
from mysql import *
import os
import unittest

from mox import Mox

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

        self.__config = FileConfig("test.conf")
        self.__config.put("drop_db_first", False)
        
    def tearDown(self):
        os.remove("test.conf")
    
    def __mock_db_init(self, mysql_driver_mock, db_mock, cursor_mock):
        mysql_driver_mock.connect(host='localhost', user='root', password='').AndReturn(db_mock)
        db_mock.set_character_set('utf-8')
        #db_mock.select_db()
        
        # create db if not exists
        #db_mock.query(query="create database if not exists migration_test;")
        #db_mock.close()
        
        # create version table if not exists
        create_version_table = "create table if not exists __db_version__ ( version varchar(20) NOT NULL default \"0\" );"
        self.__mock_db_execute(db_mock, cursor_mock, create_version_table)
        
        # check if exists any version
        db_mock.cursor().AndReturn(cursor_mock)
        cursor_mock.execute(execute="select count(*) from __db_version__;")
        cursor_mock.fetchone().AndReturn("0")
        db_mock.close()
    
    def __mock_db_execute(self, db_mock, cursor_mock, query):
        # mock a call to __execute
        db_mock.cursor().AndReturn(cursor_mock)
        
        sql_statements = query.split(";")
        sql_statements = [s.strip() for s in sql_statements if s.strip() != ""]
        for statement in sql_statements:
            cursor_mock.execute().AndReturn(statement)
            
        cursor_mock.close()
        db_mock.commit()
        db_mock.close()
    
    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        mysql = MySQL(self.__config, mysql_driver_mock)
    
    def test_it_should_drop_database_on_init_if_its_asked(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        db_mock.query(query="set foreign_key_checks=0; drop database if exists migration_test;")
        db_mock.close()

        config = FileConfig("test.conf")
        config.put("drop_db_first", True)
        
        mox.ReplayAll()

        mysql = MySQL(config, mysql_driver=mysql_driver_mock)
        
        mox.VerifyAll()
    
    def test_it_should_execute_migration_up_and_update_schema_version(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()

        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        self.__mock_db_execute(db_mock, cursor_mock, "create table spam();")
        self.__mock_db_execute(db_mock, cursor_mock, "insert into __db_version__ (version) values (\"20090212112104\");")
        
        mox.ReplayAll()

        mysql = MySQL(self.__config, mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104")
        
        mox.VerifyAll()
        
    def test_it_should_execute_migration_down_and_update_schema_version(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()

        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        self.__mock_db_execute(db_mock, cursor_mock, "create table spam();")
        self.__mock_db_execute(db_mock, cursor_mock, "delete from __db_version__ where version >= \"20090212112104\";")

        mox.ReplayAll()
        
        mysql = MySQL(self.__config, mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104", False)
        
        mox.VerifyAll()
    
    def test_it_should_get_current_schema_version(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        db_mock.cursor().AndReturn(cursor_mock)
        cursor_mock.execute(execute="select version from __db_version__ order by version desc limit 0,1;")
        cursor_mock.fetchone("0")
        db_mock.close()

        mox.ReplayAll()
        
        mysql = MySQL(self.__config, mysql_driver_mock)
        self.assertEquals("0", mysql.get_current_schema_version())
        
        mox.VerifyAll()
    
    def test_it_should_get_all_schema_versions(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        expected_versions = []
        expected_versions.append("0")
        expected_versions.append("20090211120001")
        expected_versions.append("20090211120002")
        expected_versions.append("20090211120003")
        
        db_mock.cursor(cursor_mock)
        cursor_mock.execute(execute="select version from __db_version__ order by version;")
        cursor_mock.fetchall(tuple(zip(expected_versions)))
        db_mock.close()
        
        mox.ReplayAll()
        
        mysql = MySQL(self.__config, mysql_driver_mock)
        
        schema_versions = mysql.get_all_schema_versions()
        self.assertEquals(len(expected_versions), len(schema_versions))
        for version in schema_versions:
            self.assertTrue(version in expected_versions)
            
        mox.VerifyAll()
            
    def test_it_should_parse_sql_statements(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        mox.ReplayAll()
        
        mysql = MySQL(self.__config, mysql_driver_mock)
        
        sql = 'create table eggs; drop table spam; ; ;'
        statements = mysql._parse_sql_statements(sql)
        
        assert len(statements) == 2
        assert statements[0] == 'create table eggs'
        assert statements[1] == 'drop table spam'
        
        mox.VerifyAll()
        
    def test_it_should_parse_sql_statements_with_html_inside(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        
        self.__mock_db_init(mysql_driver_mock, db_mock, cursor_mock)
        
        mox.ReplayAll()
        import pdb;pdb.set_trace()
        mysql = MySQL(self.__config, mysql_driver_mock)
        
        sql = u"""
        create table eggs;
        INSERT INTO widget_parameter_domain (widget_parameter_id, label, value)
        VALUES ((SELECT MAX(widget_parameter_id)
                FROM widget_parameter),  "Carros", '<div class="box-zap-geral">

            <div class="box-zap box-zap-autos">
                <a class="logo" target="_blank" title="ZAP" href="http://www.zap.com.br/Parceiros/g1/RedirG1.aspx?CodParceriaLink=42&amp;URL=http://www.zap.com.br">');
        drop table spam;
        """
        statements = mysql._parse_sql_statements(sql)
        
        expected_sql_with_html = """INSERT INTO widget_parameter_domain (widget_parameter_id, label, value)
        VALUES ((SELECT MAX(widget_parameter_id)
                FROM widget_parameter),  "Carros", '<div class="box-zap-geral">

            <div class="box-zap box-zap-autos">
                <a class="logo" target="_blank" title="ZAP" href="http://www.zap.com.br/Parceiros/g1/RedirG1.aspx?CodParceriaLink=42&amp;URL=http://www.zap.com.br">')"""
        
        assert len(statements) == 3, 'expected %s, got %s' % (3, len(statements))
        assert statements[0] == 'create table eggs'
        assert statements[1] == expected_sql_with_html, 'expected "%s", got "%s"' % (expected_sql_with_html, statements[1])
        assert statements[2] == 'drop table spam'
        
        mox.VerifyAll()

if __name__ == "__main__":
    unittest.main()

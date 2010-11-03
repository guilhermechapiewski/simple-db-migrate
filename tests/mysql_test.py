from mox import Mox, MockObject
import os
import unittest

from simple_db_migrate.config import *
from simple_db_migrate.mysql import *

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

    def tearDown(self):
        os.remove("test.conf")

    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)


        mox.ReplayAll()

        mysql = MySQL(config_mock, mysql_driver_mock)

        mox.VerifyAll()

    def test_it_should_drop_database_on_init_if_its_asked(self):
        mox = Mox()
        mysql_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query("set foreign_key_checks=0; drop database if exists migration_test;")
        db_mock.close()

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)


        config = FileConfig("test.conf")
        config.put("drop_db_first", True)

        mox.ReplayAll()

        mysql = MySQL(config, mysql_driver=mysql_driver_mock)

        mox.VerifyAll()

    def test_it_should_execute_migration_up_and_update_schema_version(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute("create table spam()")
        cursor_mock.close()

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('insert into __db_version__ (version, name, sql_up, sql_down) values ("20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;")')
        cursor_mock.close()

        db_mock.commit()
        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        mox.ReplayAll()
        mysql = MySQL(config_mock, mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;")

        mox.VerifyAll()

    def test_it_should_execute_migration_down_and_update_schema_version(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('create table spam()')
        cursor_mock.close()

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('delete from __db_version__ where version = "20090212112104"')
        cursor_mock.close()

        db_mock.commit()
        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        mox.ReplayAll()

        mysql = MySQL(config_mock, mysql_driver_mock)
        mysql.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", False)

        mox.VerifyAll()

    def test_it_should_get_current_schema_version(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute("select version from __db_version__ order by id desc limit 0,1;")
        cursor_mock.fetchone().AndReturn("0")
        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        mox.ReplayAll()

        mysql = MySQL(config_mock, mysql_driver_mock)

        self.assertEquals("0", mysql.get_current_schema_version())

        mox.VerifyAll()

    def test_it_should_get_all_schema_versions(self):
        mox = Mox()

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        expected_versions = []
        expected_versions.append("0")
        expected_versions.append("20090211120001")
        expected_versions.append("20090211120002")
        expected_versions.append("20090211120003")

        cursor_mock.execute('select version from __db_version__ order by id;')
        cursor_mock.fetchall().AndReturn(tuple(zip(expected_versions)))
        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        config_mock = self.create_config_mock(mox)

        mox.ReplayAll()
        mysql = MySQL(config_mock, mysql_driver_mock)

        schema_versions = mysql.get_all_schema_versions()

        self.assertEquals(len(expected_versions), len(schema_versions))
        for version in schema_versions:
            self.assertTrue(version in expected_versions)

        mox.VerifyAll()

    def test_it_should_parse_sql_statements(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        mox.ReplayAll()

        mysql = MySQL(config_mock, mysql_driver_mock)

        sql = 'create table eggs; drop table spam; ; ;'
        statements = mysql._parse_sql_statements(sql)

        assert len(statements) == 2
        assert statements[0] == 'create table eggs'
        assert statements[1] == 'drop table spam'

        mox.VerifyAll()

    def create_config_mock(self, mox):
        config_mock = mox.CreateMockAnything()
        config_mock.get('db_script_encoding', 'utf8').AndReturn('utf8')
        config_mock.get('db_encoding', 'utf8').AndReturn('utf8')
        config_mock.get('db_host').AndReturn('localhost')
        config_mock.get('db_user').AndReturn('root')
        config_mock.get('db_password').AndReturn('')
        config_mock.get('db_name').AndReturn('migration_test')
        config_mock.get('db_version_table').AndReturn('__db_version__')
        config_mock.get('drop_db_first').AndReturn(False)
        return config_mock

    def test_it_should_parse_sql_statements_with_html_inside(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        mox.ReplayAll()

        mysql = MySQL(config_mock, mysql_driver_mock)

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

    def test_it_should_get_none_for_a_non_existent_version_in_database(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)

        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))')
        cursor_mock.close()

        db_mock = mox.CreateMockAnything()
        db_mock.set_character_set('utf8')
        db_mock.query('create database if not exists migration_test;')
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')

        db_mock.cursor().AndReturn(cursor_mock)

        db_mock.commit()
        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__;')
        cursor_mock.close()

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select count(*) from __db_version__;')
        cursor_mock.fetchone().AndReturn([1])

        db_mock.close()

        db_mock.set_character_set('utf8')
        db_mock.select_db('migration_test')
        db_mock.cursor().AndReturn(cursor_mock)

        cursor_mock.execute('select id from __db_version__ where version = \'xxx\';')
        cursor_mock.fetchone().AndReturn(None)

        db_mock.close()

        mysql_driver_mock = mox.CreateMockAnything()
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)
        mysql_driver_mock.connect(host='localhost', passwd='', user='root').AndReturn(db_mock)

        mox.ReplayAll()

        mysql = MySQL(config_mock, mysql_driver_mock)

        ret = mysql.get_version_id_from_version_number('xxx')

        assert None == ret, 'expected %s, got %s' % (None, ret)

        mox.VerifyAll()



if __name__ == "__main__":
    unittest.main()

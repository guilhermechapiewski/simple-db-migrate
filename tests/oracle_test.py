from mox import Mox, MockObject
import os
import unittest

from config import *
from oracle import *

class OracleTest(unittest.TestCase):

    def setUp(self):
        config_file = """
HOST = os.getenv("DB_HOST") or "SID"
USERNAME = os.getenv("DB_USERNAME") or "root"
PASSWORD = os.getenv("DB_PASSWORD") or ""
DATABASE = os.getenv("DB_DATABASE") or "migration_test"
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR") or "."

VERSION_TABLE = os.getenv("DB_VERSION_TABLE") or "db_version"
DATABASE_ENGINE = os.getenv("DB_ENGINE") or "oracle"
"""
        f = open("test.conf", "w")
        f.write(config_file)
        f.close()
        
    def tearDown(self):
        os.remove("test.conf")
        
    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        mox = Mox()
        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndRaise(Exception("could not connect to database: ORA-01017 invalid user/password"))
        
        std_in_mock = mox.CreateMockAnything()
        std_in_mock.readline().AndReturn('dba_user\n')
        
        get_pass_mock = mox.CreateMockAnything()
        get_pass_mock().AndReturn('dba_password')
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create user root identified by ")
        cursor_mock.execute("grant connect, resource to root")
        cursor_mock.execute("grant create public synonym to root")
        cursor_mock.execute("grant drop public synonym to root")
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='dba_user', password='dba_password').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((0,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #insert the first version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("insert into db_version (version) values ('0')").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()

        oracle = Oracle(config_mock, oracle_driver_mock, get_pass_mock, std_in_mock)

        mox.VerifyAll()
        
    def test_it_should_create_version_table_on_init_if_not_exists(self):
        mox = Mox()
        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((0,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #insert the first version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("insert into db_version (version) values ('0')").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()

        oracle = Oracle(config_mock, oracle_driver_mock)

        mox.VerifyAll()
    
    def test_it_should_drop_database_on_init_if_its_asked(self):
        mox = Mox()

        oracle_driver_mock = mox.CreateMockAnything()
        db_mock = mox.CreateMockAnything()
        
        result_mock = mox.CreateMockAnything()
        result_mock.fetchall().AndReturn([("DELETE TABLE DB_VERSION CASCADE CONSTRAINTS;",),])
                
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("""\
            SELECT 'DROP PUBLIC SYNONYM ' || SYNONYM_NAME ||';' FROM ALL_SYNONYMS \
            WHERE OWNER = 'PUBLIC' AND TABLE_OWNER = '%s' \
            UNION ALL \
            SELECT 'DROP SYNONYM ' || SYNONYM_NAME ||';' FROM ALL_SYNONYMS \
            WHERE OWNER = '%s' AND TABLE_OWNER = '%s' \
            UNION ALL \
            SELECT 'DROP ' || OBJECT_TYPE || ' ' || OBJECT_NAME ||';'   FROM USER_OBJECTS \
            WHERE OBJECT_TYPE <> 'TABLE' AND OBJECT_TYPE <> 'INDEX' AND \
            OBJECT_TYPE<>'TRIGGER'  AND OBJECT_TYPE<>'LOB' \
            UNION ALL \
            SELECT 'DROP ' || OBJECT_TYPE || ' ' || OBJECT_NAME ||' CASCADE CONSTRAINTS;'   FROM USER_OBJECTS \
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')).AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("DELETE TABLE DB_VERSION CASCADE CONSTRAINTS").AndReturn(1)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((0,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #insert the first version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("insert into db_version (version) values ('0')").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        config = FileConfig("test.conf")
        config.put("drop_db_first", True)
        
        mox.ReplayAll()

        oracle = Oracle(config, oracle_driver_mock)

        mox.VerifyAll()
    
    def test_it_should_execute_migration_up_and_update_schema_version(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((1,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create table spam
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table spam()").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #update database version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("insert into db_version (version) values ('20090212112104')").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()
        
        oracle = Oracle(config_mock, oracle_driver_mock)
        oracle.change("create table spam();", "20090212112104")
        
        mox.VerifyAll()

    def test_it_should_execute_migration_down_and_update_schema_version(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((1,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #drop table spam
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("drop table spam()").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #update database version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("delete from db_version where version >= '20090212112104'").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()
        
        oracle = Oracle(config_mock, oracle_driver_mock)
        oracle.change("drop table spam();", "20090212112104", False)
        
        mox.VerifyAll()

    def test_it_should_get_current_schema_version(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((1,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #get database version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version order by version desc").AndReturn(0)
        cursor_mock.fetchone().AndReturn("0")
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()
        
        oracle = Oracle(config_mock, oracle_driver_mock)
        self.assertEquals("0", oracle.get_current_schema_version())
        
        mox.VerifyAll()

    def test_it_should_get_all_schema_versions(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((1,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        expected_versions = []
        expected_versions.append("0")
        expected_versions.append("20090211120001")
        expected_versions.append("20090211120002")
        expected_versions.append("20090211120003")
        
        #get database version
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version order by version").AndReturn(0)
        cursor_mock.fetchall().AndReturn(tuple(zip(expected_versions)))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()
        
        oracle = Oracle(config_mock, oracle_driver_mock)
        schema_versions = oracle.get_all_schema_versions()

        self.assertEquals(len(expected_versions), len(schema_versions))
        for version in schema_versions:
            self.assertTrue(version in expected_versions)
        
        mox.VerifyAll()

    def test_it_should_parse_sql_statements(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((1,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()
        
        oracle = Oracle(config_mock, oracle_driver_mock)
        
        #TODO incluir os demais tipos de sql
        sql = "create table eggs; drop table spam; ; ;\
        CREATE OR REPLACE FUNCTION simple \n\
        RETURN VARCHAR2 IS \n\
        BEGIN \n\
        RETURN 'Simple Function'; \n\
        END simple; \n\
        / \n\
        drop table eggs; \n\
        create or replace procedure proc_db_migrate(dias_fim_mes out number) \n\
        as v number; \n\
        begin \n\
            SELECT LAST_DAY(SYSDATE) - SYSDATE \"Days Left\" \n\
            into v \n\
            FROM DUAL; \n\
            dias_fim_mes := v; \n\
        end; \n\
        \t/      \n\
        create OR RePLaCe TRIGGER \"FOLDER_TR\" \n\
        BEFORE INSERT ON \"FOLDER\" \n\
        FOR EACH ROW WHEN \n\
        (\n\
            new.\"FOLDER_ID\" IS NULL \n\
        )\n\
        BEGIN\n\
            SELECT \"FOLDER_SQ\".nextval\n\
            INTO :new.\"FOLDER_ID\"\n\
            FROM dual;\n\
        EnD;\n\
        /\n\
        CREATE OR REPLACE\t PACKAGE pkg_dbm \n\
        AS \n\
        FUNCTION getArea (i_rad NUMBER) \n\
        RETURN NUMBER;\n\
            PROCEDURE p_print (i_str1 VARCHAR2 := 'hello',\n\
            i_str2 VARCHAR2 := 'world', \n\
            i_end VARCHAR2 := '!');\n\
        END;\n\
        / \n\
        CREATE OR REPLACE\n PACKAGE BODY pkg_dbm \n\
        AS \n\
            FUNCTION getArea (i_rad NUMBER) \n\
            RETURN NUMBER \n\
            IS \n\
                v_pi NUMBER := 3.14; \n\
            BEGIN \n\
                RETURN v_pi * (i_rad ** 2); \n\
            END; \n\
            PROCEDURE p_print (i_str1 VARCHAR2 := 'hello', i_str2 VARCHAR2 := 'world', i_end VARCHAR2 := '!') \n\
            IS \n\
            BEGIN \n\
                DBMS_OUTPUT.put_line (i_str1 || ',' || i_str2 || i_end); \n\
            END; \n\
        END; \n\
        / \n\
        DECLARE\n\
            counter NUMBER(10,8) := 2; \r\n\
            pi NUMBER(8,7) := 3.1415926; \n\
            test NUMBER(10,8) NOT NULL := 10;\n\
        BEGIN \n\
            counter := pi/counter; \n\
            pi := pi/3; \n\
            dbms_output.put_line(counter); \n\
            dbms_output.put_line(pi); \n\
        END; \n\
        / \n\
        BEGIN \n\
            dbms_output.put_line('teste de bloco anonimo'); \n\
            dbms_output.put_line(select 1 from dual); \n\
        END; \n\
        / "
        
        statements = oracle._parse_sql_statements(sql)

        assert len(statements) == 10
        assert statements[0] == 'create table eggs'
        assert statements[1] == 'drop table spam'
        assert statements[2] == "CREATE OR REPLACE FUNCTION simple \n\
        RETURN VARCHAR2 IS \n\
        BEGIN \n\
        RETURN 'Simple Function'; \n\
        END simple;"
        assert statements[3] == 'drop table eggs'
        assert statements[4] == 'create or replace procedure proc_db_migrate(dias_fim_mes out number) \n\
        as v number; \n\
        begin \n\
            SELECT LAST_DAY(SYSDATE) - SYSDATE \"Days Left\" \n\
            into v \n\
            FROM DUAL; \n\
            dias_fim_mes := v; \n\
        end;'
        assert statements[5] == 'create OR RePLaCe TRIGGER \"FOLDER_TR\" \n\
        BEFORE INSERT ON \"FOLDER\" \n\
        FOR EACH ROW WHEN \n\
        (\n\
            new.\"FOLDER_ID\" IS NULL \n\
        )\n\
        BEGIN\n\
            SELECT \"FOLDER_SQ\".nextval\n\
            INTO :new.\"FOLDER_ID\"\n\
            FROM dual;\n\
        EnD;'
        assert statements[6] == "CREATE OR REPLACE\t PACKAGE pkg_dbm \n\
        AS \n\
        FUNCTION getArea (i_rad NUMBER) \n\
        RETURN NUMBER;\n\
            PROCEDURE p_print (i_str1 VARCHAR2 := 'hello',\n\
            i_str2 VARCHAR2 := 'world', \n\
            i_end VARCHAR2 := '!');\n\
        END;"
        assert statements[7] == "CREATE OR REPLACE\n PACKAGE BODY pkg_dbm \n\
        AS \n\
            FUNCTION getArea (i_rad NUMBER) \n\
            RETURN NUMBER \n\
            IS \n\
                v_pi NUMBER := 3.14; \n\
            BEGIN \n\
                RETURN v_pi * (i_rad ** 2); \n\
            END; \n\
            PROCEDURE p_print (i_str1 VARCHAR2 := 'hello', i_str2 VARCHAR2 := 'world', i_end VARCHAR2 := '!') \n\
            IS \n\
            BEGIN \n\
                DBMS_OUTPUT.put_line (i_str1 || ',' || i_str2 || i_end); \n\
            END; \n\
        END;"
        assert statements[8] == "DECLARE\n\
            counter NUMBER(10,8) := 2; \r\n\
            pi NUMBER(8,7) := 3.1415926; \n\
            test NUMBER(10,8) NOT NULL := 10;\n\
        BEGIN \n\
            counter := pi/counter; \n\
            pi := pi/3; \n\
            dbms_output.put_line(counter); \n\
            dbms_output.put_line(pi); \n\
        END;"
        assert statements[9] == "BEGIN \n\
            dbms_output.put_line('teste de bloco anonimo'); \n\
            dbms_output.put_line(select 1 from dual); \n\
        END;"
        
        mox.VerifyAll()

    def test_it_should_parse_sql_statements_with_html_inside(self):
        mox = Mox()

        config_mock = self.create_config_mock(mox)
        
        oracle_driver_mock = mox.CreateMockAnything()
        
        #verify if the user exists
        db_mock = mox.CreateMockAnything()
        db_mock.close()
    
        oracle_driver_mock.connect(dsn='SID', password='', user='root').AndReturn(db_mock)
        
        #verify if the version table exists
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select version from db_version").AndRaise(Exception("Table doesn't exist'"))
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.rollback()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #create version table
        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("create table db_version ( version varchar2(20) default '0' NOT NULL )").AndReturn(0)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.commit()
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        #count number of versions
        result_mock = mox.CreateMockAnything()
        result_mock.fetchone().AndReturn((1,))

        db_mock = mox.CreateMockAnything()
        cursor_mock = mox.CreateMockAnything()
        cursor_mock.execute("select count(*) from db_version").AndReturn(result_mock)
        cursor_mock.close()
        
        db_mock.cursor().AndReturn(cursor_mock)
        db_mock.close()
        
        oracle_driver_mock.connect(dsn='SID', user='root', password='').AndReturn(db_mock)
        
        mox.ReplayAll()
        
        oracle = Oracle(config_mock, oracle_driver_mock)
        
        sql = u"""
        create table eggs;
        INSERT INTO widget_parameter_domain (widget_parameter_id, label, value)
        VALUES ((SELECT MAX(widget_parameter_id)
                FROM widget_parameter),  "Carros", '<div class="box-zap-geral">

            <div class="box-zap box-zap-autos">
                <a class="logo" target="_blank" title="ZAP" href="http://www.zap.com.br/Parceiros/g1/RedirG1.aspx?CodParceriaLink=42&amp;URL=http://www.zap.com.br">');
        drop table spam;
        """
        statements = oracle._parse_sql_statements(sql)
        
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
        
    def create_config_mock(self, mox):
        config_mock = mox.CreateMockAnything()
        config_mock.get('db_script_encoding', 'utf8').AndReturn('utf8')
        config_mock.get('db_encoding', 'American_America.UTF8').AndReturn('American_America.UTF8')
        config_mock.get('db_host').AndReturn('SID')
        config_mock.get('db_user').AndReturn('root')
        config_mock.get('db_password').AndReturn('')
        config_mock.get('db_name').AndReturn('migration_test')
        config_mock.get('db_version_table').AndReturn('db_version')
        config_mock.get('drop_db_first').AndReturn(False)
        return config_mock

if __name__ == "__main__":
    unittest.main()

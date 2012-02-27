import os
import unittest
import simple_db_migrate
from mock import patch, Mock, MagicMock, call, sentinel
from simple_db_migrate.config import *
from simple_db_migrate.oracle import *

class OracleTest(unittest.TestCase):

    def setUp(self):
        self.execute_returns = {}
        self.fetchone_returns = {'select count(*) from db_version': [0]}
        self.close_returns = {}
        self.last_execute_command = '';
        self.config_dict = {'db_script_encoding': 'utf8',
                   'db_encoding': 'American_America.UTF8',
                   'db_host': 'SID',
                   'db_user': 'root',
                   'db_password': '',
                   'db_name': 'migration_test',
                   'db_version_table': 'db_version',
                   'drop_db_first': False
                }

        self.config_mock = MagicMock(spec_set=dict, wraps=self.config_dict)
        self.cursor_mock = Mock(**{"execute": Mock(side_effect=self.execute_side_effect),
                                   "close": Mock(side_effect=self.close_side_effect),
                                   "fetchone": Mock(side_effect=self.fetchone_side_effect)})
        self.db_mock = Mock(**{"cursor.return_value": self.cursor_mock})
        self.db_driver_mock = Mock(**{"connect.return_value": self.db_mock})
        self.stdin_mock = Mock(**{"readline.return_value":"dba_user"})
        self.getpass_mock = Mock(return_value = "dba_password")

    def test_it_should_use_cx_Oracle_as_driver(self):
        cx_Oracle_mock = MagicMock()
        with patch.dict('sys.modules', cx_Oracle=cx_Oracle_mock):
            oracle = Oracle(self.config_mock)
            self.assertNotEqual(0, cx_Oracle_mock.connect.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_connect_database(self):
        self.db_driver_mock.connect.side_effect = Exception("error when connecting")

        try:
            oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("could not connect to database: error when connecting", str(e))

        self.assertEqual(0, self.db_mock.commit.call_count)
        self.assertEqual(0, self.db_mock.close.call_count)

        self.assertEqual(0, self.cursor_mock.execute.call_count)
        self.assertEqual(0, self.cursor_mock.close.call_count)

    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        self.first_return = Exception("could not connect to database: ORA-01017 invalid user/password")
        def connect_side_effect(*args, **kwargs):
            ret = sentinel.DEFAULT
            if (kwargs['user'] == 'root') and self.first_return:
                ret = self.first_return
                self.first_return = None
                raise ret
            return ret

        self.db_driver_mock.connect.side_effect = connect_side_effect
        self.execute_returns["select version from db_version"] = Exception("Table doesn't exist")

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(9, self.db_driver_mock.connect.call_count)
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(8, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create user root identified by '),
            call('grant connect, resource to root'),
            call('grant create public synonym to root'),
            call('grant drop public synonym to root'),
            call('select version from db_version'),
            call("create table db_version ( id number(11) not null, version varchar2(20) default '0' NOT NULL, label varchar2(255), name varchar2(255), sql_up clob, sql_down clob, CONSTRAINT db_version_pk PRIMARY KEY (id) ENABLE)"),
            call('drop sequence db_version_seq'),
            call('create sequence db_version_seq start with 1 increment by 1 nomaxvalue'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(8, self.cursor_mock.close.call_count)

    def test_it_should_create_version_table_on_init_if_not_exists(self):
        self.execute_returns["select version from db_version"] = Exception("Table doesn't exist")

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(8, self.db_driver_mock.connect.call_count)
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(8, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call("create table db_version ( id number(11) not null, version varchar2(20) default '0' NOT NULL, label varchar2(255), name varchar2(255), sql_up clob, sql_down clob, CONSTRAINT db_version_pk PRIMARY KEY (id) ENABLE)"),
            call('drop sequence db_version_seq'),
            call('create sequence db_version_seq start with 1 increment by 1 nomaxvalue'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(7, self.cursor_mock.close.call_count)

    def test_it_should_drop_database_on_init_if_its_asked(self):
        select_elements_to_drop_sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')

        self.config_dict["drop_db_first"] = True
        self.cursor_mock.fetchall.return_value = [("DELETE TABLE DB_VERSION CASCADE CONSTRAINTS;",),]
        self.execute_returns["select version from db_version"] = Exception("Table doesn't exist")

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(10, self.db_driver_mock.connect.call_count)
        self.assertEqual(5, self.db_mock.commit.call_count)
        self.assertEqual(10, self.db_mock.close.call_count)

        expected_execute_calls = [
            call(select_elements_to_drop_sql),
            call('DELETE TABLE DB_VERSION CASCADE CONSTRAINTS'),
            call('select version from db_version'),
            call("create table db_version ( id number(11) not null, version varchar2(20) default '0' NOT NULL, label varchar2(255), name varchar2(255), sql_up clob, sql_down clob, CONSTRAINT db_version_pk PRIMARY KEY (id) ENABLE)"),
            call('drop sequence db_version_seq'),
            call('create sequence db_version_seq start with 1 increment by 1 nomaxvalue'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(9, self.cursor_mock.close.call_count)

    def test_it_should_create_user_when_it_does_not_exists_during_drop_database_selecting_elements_to_drop(self):
        select_elements_to_drop_sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')

        self.config_dict["drop_db_first"] = True
        self.execute_returns[select_elements_to_drop_sql] = Exception("could not connect to database: ORA-01017 invalid user/password")

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(7, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(7, self.db_mock.close.call_count)

        expected_execute_calls = [
            call(select_elements_to_drop_sql),
            call('create user root identified by '),
            call('grant connect, resource to root'),
            call('grant create public synonym to root'),
            call('grant drop public synonym to root'),
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(6, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_create_user(self):
        select_elements_to_drop_sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')

        self.config_dict["drop_db_first"] = True
        self.execute_returns[select_elements_to_drop_sql] = Exception("could not connect to database: ORA-01017 invalid user/password")
        self.execute_returns['grant create public synonym to root'] = Exception("error when granting")

        try:
            oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("check error: error when granting", str(e))

        self.assertEqual(2, self.db_driver_mock.connect.call_count)
        self.assertEqual(0, self.db_mock.commit.call_count)
        self.assertEqual(2, self.db_mock.close.call_count)

        expected_execute_calls = [
            call(select_elements_to_drop_sql),
            call('create user root identified by '),
            call('grant connect, resource to root'),
            call('grant create public synonym to root')
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(2, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_drop_database_selecting_elements_to_drop(self):
        select_elements_to_drop_sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')

        self.config_dict["drop_db_first"] = True
        self.execute_returns[select_elements_to_drop_sql] = Exception("error when dropping")

        try:
            oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("error when dropping", str(e))

        self.assertEqual(0, self.db_mock.commit.call_count)
        self.assertEqual(1, self.db_mock.close.call_count)

        expected_execute_calls = [
            call(select_elements_to_drop_sql)
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(1, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_drop_elements_from_database_and_user_asked_to_stop(self):
        select_elements_to_drop_sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')

        self.config_dict["drop_db_first"] = True
        self.cursor_mock.fetchall.return_value = [("DELETE TABLE DB_VERSION CASCADE CONSTRAINTS;",),("DELETE TABLE AUX CASCADE CONSTRAINTS;",)]
        self.execute_returns["DELETE TABLE DB_VERSION CASCADE CONSTRAINTS"] = Exception("error dropping table")
        self.stdin_mock.readline.return_value = "n"

        try:
            oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("can't drop database 'migration_test'", str(e))

        self.assertEqual(1, self.db_mock.commit.call_count)
        self.assertEqual(3, self.db_mock.close.call_count)

        expected_execute_calls = [
            call(select_elements_to_drop_sql),
            call('DELETE TABLE DB_VERSION CASCADE CONSTRAINTS'),
            call('DELETE TABLE AUX CASCADE CONSTRAINTS')
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(3, self.cursor_mock.close.call_count)

    def test_it_should_not_stop_process_when_an_error_occur_during_drop_elements_from_database_and_user_asked_to_continue(self):
        select_elements_to_drop_sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % ('ROOT','ROOT','ROOT')

        self.config_dict["drop_db_first"] = True
        self.cursor_mock.fetchall.return_value = [("DELETE TABLE DB_VERSION CASCADE CONSTRAINTS;",),("DELETE TABLE AUX CASCADE CONSTRAINTS;",)]
        self.execute_returns["DELETE TABLE DB_VERSION CASCADE CONSTRAINTS"] = Exception("error dropping table")
        self.stdin_mock.readline.return_value = "y"

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(3, self.db_mock.commit.call_count)
        self.assertEqual(8, self.db_mock.close.call_count)

        expected_execute_calls = [
            call(select_elements_to_drop_sql),
            call('DELETE TABLE DB_VERSION CASCADE CONSTRAINTS'),
            call('DELETE TABLE AUX CASCADE CONSTRAINTS'),
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(7, self.cursor_mock.close.call_count)

    def test_it_should_execute_migration_up_and_update_schema_version(self):
        self.db_driver_mock.CLOB = 'X'

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        oracle.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;")

        self.assertEqual(7, self.db_driver_mock.connect.call_count)
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(7, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('create table spam()'),
            call('insert into db_version (id, version, label, name, sql_up, sql_down) values (db_version_seq.nextval, :version, :label, :migration_file_name, :sql_up, :sql_down)', {'label': None, 'sql_up': 'create table spam();', 'version': '20090212112104', 'sql_down': 'drop table spam;', 'migration_file_name': '20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration'})
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(6, self.cursor_mock.close.call_count)

        expected_var_calls = [
            call('X', 20),
            call().setvalue(0, 'create table spam();'),
            call('X', 16),
            call().setvalue(0, 'drop table spam;')
        ]
        self.assertEqual(expected_var_calls, self.cursor_mock.var.mock_calls)

    def test_it_should_execute_migration_down_and_update_schema_version(self):
        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        oracle.change("drop table spam;", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", False)

        self.assertEqual(7, self.db_driver_mock.connect.call_count)
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(7, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('drop table spam'),
            call('delete from db_version where version = :version', {'version': '20090212112104'})
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(6, self.cursor_mock.close.call_count)


    def test_it_should_use_label_version_when_updating_schema_version(self):
        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        oracle.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", label_version="label")

        self.assertEqual(7, self.db_driver_mock.connect.call_count)
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(7, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('create table spam()'),
            call('insert into db_version (id, version, label, name, sql_up, sql_down) values (db_version_seq.nextval, :version, :label, :migration_file_name, :sql_up, :sql_down)', {'label': "label", 'sql_up': 'create table spam();', 'version': '20090212112104', 'sql_down': 'drop table spam;', 'migration_file_name': '20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration'})
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(6, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_database_change(self):
        self.execute_returns["insert into spam"] = Exception("invalid sql")

        try:
            oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
            oracle.change("create table spam(); insert into spam", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", label_version="label")
        except Exception, e:
            self.assertEqual("error executing migration: invalid sql\n\n[ERROR DETAILS] SQL command was:\ninsert into spam", str(e))
            self.assertTrue(isinstance(e, simple_db_migrate.core.exceptions.MigrationException))

        self.assertEqual(1, self.db_mock.rollback.call_count)
        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('create table spam()'),
            call('insert into spam')
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_log_schema_version(self):
        self.execute_returns['insert into db_version (id, version, label, name, sql_up, sql_down) values (db_version_seq.nextval, :version, :label, :migration_file_name, :sql_up, :sql_down)'] = Exception("invalid sql")

        try:
            oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
            oracle.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", label_version="label")
        except Exception, e:
            self.assertEqual('error logging migration: invalid sql\n\n[ERROR DETAILS] SQL command was:\n20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration', str(e))
            self.assertTrue(isinstance(e, simple_db_migrate.core.exceptions.MigrationException))

        self.assertEqual(7, self.db_driver_mock.connect.call_count)
        self.assertEqual(1, self.db_mock.rollback.call_count)
        self.assertEqual(3, self.db_mock.commit.call_count)
        self.assertEqual(7, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('create table spam()'),
            call('insert into db_version (id, version, label, name, sql_up, sql_down) values (db_version_seq.nextval, :version, :label, :migration_file_name, :sql_up, :sql_down)', {'label': 'label', 'sql_up': 'create table spam();', 'version': '20090212112104', 'sql_down': 'drop table spam;', 'migration_file_name': '20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration'})
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_log_execution_when_a_function_is_given_when_updating_schema_version(self):
        execution_log_mock = Mock()
        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        oracle.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", execution_log=execution_log_mock)

        expected_execution_log_calls = [
            call('create table spam()\n-- 0 row(s) affected\n'),
            call('migration 20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration registered\n')
        ]
        self.assertEqual(expected_execution_log_calls, execution_log_mock.mock_calls)


    def test_it_should_get_current_schema_version(self):
        self.fetchone_returns = {'select count(*) from db_version': [0], 'select version from db_version order by id desc': ["0"]}

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        self.assertEquals("0", oracle.get_current_schema_version())


        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('select version from db_version order by id desc')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_get_all_schema_versions(self):
        expected_versions = []
        expected_versions.append("0")
        expected_versions.append("20090211120001")
        expected_versions.append("20090211120002")
        expected_versions.append("20090211120003")

        self.cursor_mock.fetchall.return_value = tuple(zip(expected_versions))

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        schema_versions = oracle.get_all_schema_versions()

        self.assertEquals(len(expected_versions), len(schema_versions))
        for version in schema_versions:
            self.assertTrue(version in expected_versions)

        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('select version from db_version order by id')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_get_all_schema_migrations(self):
        expected_versions = []
        expected_versions.append([1, "0", None, None, None, None])
        expected_versions.append([2, "20090211120001", "label", "20090211120001_name", Mock(**{"read.return_value":"sql_up"}), Mock(**{"read.return_value":"sql_down"})])

        self.cursor_mock.fetchall.return_value = tuple(expected_versions)

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        schema_migrations = oracle.get_all_schema_migrations()

        self.assertEquals(len(expected_versions), len(schema_migrations))
        for index, migration in enumerate(schema_migrations):
            self.assertEqual(migration.id, expected_versions[index][0])
            self.assertEqual(migration.version, expected_versions[index][1])
            self.assertEqual(migration.label, expected_versions[index][2])
            self.assertEqual(migration.file_name, expected_versions[index][3])
            self.assertEqual(migration.sql_up, expected_versions[index][4] and expected_versions[index][4].read() or "")
            self.assertEqual(migration.sql_down, expected_versions[index][5] and expected_versions[index][5].read() or "")

        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call('select id, version, label, name, sql_up, sql_down from db_version order by id')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)


    def test_it_should_parse_sql_statements(self):
        #TODO include other types of sql
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

        statements = Oracle._parse_sql_statements(sql)

        self.assertEqual(10, len(statements))
        self.assertEqual('create table eggs', statements[0])
        self.assertEqual('drop table spam', statements[1])
        self.assertEqual("CREATE OR REPLACE FUNCTION simple \n\
        RETURN VARCHAR2 IS \n\
        BEGIN \n\
        RETURN 'Simple Function'; \n\
        END simple;", statements[2])
        self.assertEqual('drop table eggs', statements[3])
        self.assertEqual('create or replace procedure proc_db_migrate(dias_fim_mes out number) \n\
        as v number; \n\
        begin \n\
            SELECT LAST_DAY(SYSDATE) - SYSDATE \"Days Left\" \n\
            into v \n\
            FROM DUAL; \n\
            dias_fim_mes := v; \n\
        end;', statements[4])
        self.assertEqual('create OR RePLaCe TRIGGER \"FOLDER_TR\" \n\
        BEFORE INSERT ON \"FOLDER\" \n\
        FOR EACH ROW WHEN \n\
        (\n\
            new.\"FOLDER_ID\" IS NULL \n\
        )\n\
        BEGIN\n\
            SELECT \"FOLDER_SQ\".nextval\n\
            INTO :new.\"FOLDER_ID\"\n\
            FROM dual;\n\
        EnD;', statements[5])
        self.assertEqual("CREATE OR REPLACE\t PACKAGE pkg_dbm \n\
        AS \n\
        FUNCTION getArea (i_rad NUMBER) \n\
        RETURN NUMBER;\n\
            PROCEDURE p_print (i_str1 VARCHAR2 := 'hello',\n\
            i_str2 VARCHAR2 := 'world', \n\
            i_end VARCHAR2 := '!');\n\
        END;", statements[6])
        self.assertEqual("CREATE OR REPLACE\n PACKAGE BODY pkg_dbm \n\
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
        END;", statements[7])
        self.assertEqual("DECLARE\n\
            counter NUMBER(10,8) := 2; \r\n\
            pi NUMBER(8,7) := 3.1415926; \n\
            test NUMBER(10,8) NOT NULL := 10;\n\
        BEGIN \n\
            counter := pi/counter; \n\
            pi := pi/3; \n\
            dbms_output.put_line(counter); \n\
            dbms_output.put_line(pi); \n\
        END;", statements[8])
        self.assertEqual("BEGIN \n\
            dbms_output.put_line('teste de bloco anonimo'); \n\
            dbms_output.put_line(select 1 from dual); \n\
        END;", statements[9])

    def test_it_should_parse_sql_statements_with_html_inside(self):

        sql = u"""
        create table eggs;
        INSERT INTO widget_parameter_domain (widget_parameter_id, label, value)
        VALUES ((SELECT MAX(widget_parameter_id)
                FROM widget_parameter),  "Carros", '<div class="box-zap-geral">

            <div class="box-zap box-zap-autos">
                <a class="logo" target="_blank" title="ZAP" href="http://www.zap.com.br/Parceiros/g1/RedirG1.aspx?CodParceriaLink=42&amp;URL=http://www.zap.com.br">');
        drop table spam;
        """
        statements = Oracle._parse_sql_statements(sql)

        expected_sql_with_html = """INSERT INTO widget_parameter_domain (widget_parameter_id, label, value)
        VALUES ((SELECT MAX(widget_parameter_id)
                FROM widget_parameter),  "Carros", '<div class="box-zap-geral">

            <div class="box-zap box-zap-autos">
                <a class="logo" target="_blank" title="ZAP" href="http://www.zap.com.br/Parceiros/g1/RedirG1.aspx?CodParceriaLink=42&amp;URL=http://www.zap.com.br">')"""

        self.assertEqual(3, len(statements))
        self.assertEqual('create table eggs', statements[0])
        self.assertEqual(expected_sql_with_html, statements[1])
        self.assertEqual('drop table spam', statements[2])

    def test_it_should_get_none_for_a_non_existent_version_in_database(self):
        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        ret = oracle.get_version_id_from_version_number('xxx')

        self.assertEqual(None, ret)

        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call("select id from db_version where version = 'xxx'")
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_get_most_recent_version_for_a_existent_label_in_database(self):
        self.fetchone_returns["select version from db_version where label = 'xxx' order by id desc"] = ["vesion", "version2", "version3"]

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        ret = oracle.get_version_number_from_label('xxx')

        self.assertEqual("vesion", ret)

        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call("select version from db_version where label = 'xxx' order by id desc")
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_get_none_for_a_non_existent_label_in_database(self):
        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)
        ret = oracle.get_version_number_from_label('xxx')

        self.assertEqual(None, ret)

        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')"),
            call("select version from db_version where label = 'xxx' order by id desc")
        ]

        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_update_version_table_on_init_if_dont_have_id_field(self):
        self.execute_returns["select version from db_version"] = ["0"]
        self.execute_returns["select id from db_version"] = Exception("Don't have id field")

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(8, self.db_driver_mock.connect.call_count)
        self.assertEqual(5, self.db_mock.commit.call_count)
        self.assertEqual(8, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('alter table db_version add (id number(11), name varchar2(255), sql_up clob, sql_down clob)'),
            call('drop sequence db_version_seq'),
            call('create sequence db_version_seq start with 1 increment by 1 nomaxvalue'),
            call('update db_version set id = db_version_seq.nextval'),
            call('alter table db_version add constraint db_version_pk primary key (id)'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(7, self.cursor_mock.close.call_count)

    def test_it_should_update_version_table_on_init_if_dont_have_label_field(self):
        self.execute_returns["select version from db_version"] = ["0"]
        self.execute_returns["select label from db_version"] = Exception("Don't have label field")

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(6, self.db_driver_mock.connect.call_count)
        self.assertEqual(3, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version add (label varchar2(255))'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_update_version_table_on_init_dropping_label_constraint_if_still_have_it(self):
        self.execute_returns["select version from db_version"] = ["0"]

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(5, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_not_stop_when_an_error_occur_during_dropping_label_constraint(self):
        self.execute_returns["select version from db_version"] = ["0"]
        self.execute_returns['alter table db_version drop constraint db_version_uk_label'] = Exception('error dropping label constraint')

        oracle = Oracle(self.config_mock, self.db_driver_mock, self.getpass_mock, self.stdin_mock)

        self.assertEqual(5, self.db_driver_mock.connect.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('select version from db_version'),
            call('select id from db_version'),
            call('select label from db_version'),
            call('alter table db_version drop constraint db_version_uk_label'),
            call('select count(*) from db_version'),
            call("insert into db_version (id, version) values (db_version_seq.nextval, '0')")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def side_effect(self, returns, default_value):
        result = returns.get(self.last_execute_command, default_value)
        if isinstance(result, Exception):
            raise result
        return result

    def execute_side_effect(self, *args):
        self.last_execute_command = args[0]
        return self.side_effect(self.execute_returns, 0)

    def fetchone_side_effect(self, *args):
        return self.side_effect(self.fetchone_returns, None)

    def close_side_effect(self, *args):
        return self.side_effect(self.close_returns, None)

if __name__ == "__main__":
    unittest.main()

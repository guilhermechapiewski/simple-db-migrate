import os
import unittest
import simple_db_migrate
from mock import patch, Mock, MagicMock, call
from simple_db_migrate.config import *
from simple_db_migrate.mysql import *

class MySQLTest(unittest.TestCase):

    def setUp(self):
        self.execute_returns = {}
        self.fetchone_returns = {'select count(*) from __db_version__;': [0]}
        self.close_returns = {}
        self.last_execute_command = '';
        self.config_dict = {'database_script_encoding': 'utf8',
                   'database_encoding': 'utf8',
                   'database_host': 'localhost',
                   'database_user': 'root',
                   'database_password': '',
                   'database_name': 'migration_test',
                   'database_version_table': '__db_version__',
                   'drop_db_first': False
                }

        self.config_mock = MagicMock(spec_set=dict, wraps=self.config_dict)
        self.cursor_mock = Mock(**{"execute": Mock(side_effect=self.execute_side_effect),
                                   "close": Mock(side_effect=self.close_side_effect),
                                   "fetchone": Mock(side_effect=self.fetchone_side_effect)})
        self.db_mock = Mock(**{"cursor.return_value": self.cursor_mock})
        self.db_driver_mock = Mock(**{"connect.return_value": self.db_mock})

    def test_it_should_use_mysqldb_as_driver(self):
        mysqldb_mock = MagicMock()
        with patch.dict('sys.modules', MySQLdb=mysqldb_mock):
            mysql = MySQL(self.config_mock)
            self.assertNotEqual(0, mysqldb_mock.connect.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_connect_database(self):
        self.db_driver_mock.connect.side_effect = Exception("error when connecting")

        try:
            mysql = MySQL(self.config_mock, self.db_driver_mock)
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("could not connect to database: error when connecting", str(e))

        self.assertEqual(0, self.db_mock.query.call_count)
        self.assertEqual(0, self.db_mock.commit.call_count)
        self.assertEqual(0, self.db_mock.close.call_count)

        self.assertEqual(0, self.cursor_mock.execute.call_count)
        self.assertEqual(0, self.cursor_mock.close.call_count)


    def test_it_should_create_database_and_version_table_on_init_if_not_exists(self):
        mysql = MySQL(self.config_mock, self.db_driver_mock)

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(4, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(3, self.cursor_mock.close.call_count)

    def test_it_should_drop_database_on_init_if_its_asked(self):
        self.config_dict["drop_db_first"] = True

        mysql = MySQL(self.config_mock, self.db_driver_mock)

        expected_query_calls = [
            call('set foreign_key_checks=0; drop database if exists `migration_test`;'),
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(3, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_drop_database(self):
        self.config_dict["drop_db_first"] = True
        self.db_mock.query.side_effect = Exception("error when dropping")

        try:
            mysql = MySQL(self.config_mock, self.db_driver_mock)
            self.fail("it should not get here")
        except Exception, e:
            self.assertEqual("can't drop database 'migration_test'; \nerror when dropping", str(e))

        expected_query_calls = [
            call('set foreign_key_checks=0; drop database if exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.assertEqual(0, self.db_mock.commit.call_count)
        self.assertEqual(1, self.db_mock.close.call_count)

        self.assertEqual(0, self.cursor_mock.execute.call_count)
        self.assertEqual(0, self.cursor_mock.close.call_count)

    def test_it_should_execute_migration_up_and_update_schema_version(self):
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        mysql.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;")

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('create table spam()'),
            call('insert into __db_version__ (version, label, name, sql_up, sql_down) values ("20090212112104", NULL, "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;");')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_execute_migration_down_and_update_schema_version(self):
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        mysql.change("drop table spam;", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", False)

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('drop table spam'),
            call('delete from __db_version__ where version = "20090212112104";')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_use_label_version_when_updating_schema_version(self):
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        mysql.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", label_version="label")

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(4, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('create table spam()'),
            call('insert into __db_version__ (version, label, name, sql_up, sql_down) values ("20090212112104", "label", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;");')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(5, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_database_change(self):
        self.execute_returns["insert into spam"] = Exception("invalid sql")

        try:
            mysql = MySQL(self.config_mock, self.db_driver_mock)
            mysql.change("create table spam(); insert into spam", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", label_version="label")
        except Exception, e:
            self.assertEqual("error executing migration: invalid sql\n\n[ERROR DETAILS] SQL command was:\ninsert into spam", str(e))
            self.assertTrue(isinstance(e, simple_db_migrate.core.exceptions.MigrationException))

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(1, self.db_mock.rollback.call_count)
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('create table spam()'),
            call('insert into spam')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(3, self.cursor_mock.close.call_count)

    def test_it_should_stop_process_when_an_error_occur_during_log_schema_version(self):
        self.execute_returns['insert into __db_version__ (version, label, name, sql_up, sql_down) values ("20090212112104", "label", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;");'] = Exception("invalid sql")

        try:
            mysql = MySQL(self.config_mock, self.db_driver_mock)
            mysql.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", label_version="label")
        except Exception, e:
            self.assertEqual('error logging migration: invalid sql\n\n[ERROR DETAILS] SQL command was:\n20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration', str(e))
            self.assertTrue(isinstance(e, simple_db_migrate.core.exceptions.MigrationException))

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(1, self.db_mock.rollback.call_count)
        self.assertEqual(3, self.db_mock.commit.call_count)
        self.assertEqual(6, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('create table spam()'),
            call('insert into __db_version__ (version, label, name, sql_up, sql_down) values ("20090212112104", "label", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;");')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_log_execution_when_a_function_is_given_when_updating_schema_version(self):
        execution_log_mock = Mock()
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        mysql.change("create table spam();", "20090212112104", "20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration", "create table spam();", "drop table spam;", execution_log=execution_log_mock)

        expected_execution_log_calls = [
            call('create table spam()\n-- 0 row(s) affected\n'),
            call('migration 20090212112104_test_it_should_execute_migration_down_and_update_schema_version.migration registered\n')
        ]
        self.assertEqual(expected_execution_log_calls, execution_log_mock.mock_calls)

    def test_it_should_get_current_schema_version(self):
        self.fetchone_returns = {'select count(*) from __db_version__;': [0], 'select version from __db_version__ order by id desc limit 0,1;': ["0"]}

        mysql = MySQL(self.config_mock, self.db_driver_mock)
        self.assertEqual("0", mysql.get_current_schema_version())

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('select version from __db_version__ order by id desc limit 0,1;')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_get_all_schema_versions(self):
        expected_versions = []
        expected_versions.append("0")
        expected_versions.append("20090211120001")
        expected_versions.append("20090211120002")
        expected_versions.append("20090211120003")

        self.cursor_mock.fetchall.return_value = tuple(zip(expected_versions))

        mysql = MySQL(self.config_mock, self.db_driver_mock)
        schema_versions = mysql.get_all_schema_versions()

        self.assertEquals(len(expected_versions), len(schema_versions))
        for version in schema_versions:
            self.assertTrue(version in expected_versions)

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('select version from __db_version__ order by id;')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_get_all_schema_migrations(self):
        expected_versions = []
        expected_versions.append([1, "0", None, None, None, None])
        expected_versions.append([2, "20090211120001", "label", "20090211120001_name", "sql_up", "sql_down"])

        self.cursor_mock.fetchall.return_value = tuple(expected_versions)

        mysql = MySQL(self.config_mock, self.db_driver_mock)
        schema_migrations = mysql.get_all_schema_migrations()

        self.assertEquals(len(expected_versions), len(schema_migrations))
        for index, migration in enumerate(schema_migrations):
            self.assertEqual(migration.id, expected_versions[index][0])
            self.assertEqual(migration.version, expected_versions[index][1])
            self.assertEqual(migration.label, expected_versions[index][2])
            self.assertEqual(migration.file_name, expected_versions[index][3])
            self.assertEqual(migration.sql_up, expected_versions[index][4] and expected_versions[index][4] or "")
            self.assertEqual(migration.sql_down, expected_versions[index][5] and expected_versions[index][5] or "")

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call('select id, version, label, name, sql_up, sql_down from __db_version__ order by id;')
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_parse_sql_statements(self):
        statements = MySQL._parse_sql_statements('; ; create table eggs; drop table spam; ; ;')

        self.assertEqual(2, len(statements))
        self.assertEqual('create table eggs', statements[0])
        self.assertEqual('drop table spam', statements[1])

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
        statements = MySQL._parse_sql_statements(sql)

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
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        ret = mysql.get_version_id_from_version_number('xxx')
        self.assertEqual(None, ret)

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call("select id from __db_version__ where version = 'xxx';")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_get_most_recent_version_for_a_existent_label_in_database(self):
        self.fetchone_returns["select version from __db_version__ where label = 'xxx' order by id desc"] = ["vesion", "version2", "version3"]
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        ret = mysql.get_version_number_from_label('xxx')
        self.assertEqual("vesion", ret)

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call("select version from __db_version__ where label = 'xxx' order by id desc")
        ]
        self.assertEqual(expected_execute_calls, self.cursor_mock.execute.mock_calls)
        self.assertEqual(4, self.cursor_mock.close.call_count)

    def test_it_should_get_none_for_a_non_existent_label_in_database(self):
        mysql = MySQL(self.config_mock, self.db_driver_mock)
        ret = mysql.get_version_number_from_label('xxx')
        self.assertEqual(None, ret)

        expected_query_calls = [
            call('create database if not exists `migration_test`;')
        ]
        self.assertEqual(expected_query_calls, self.db_mock.query.mock_calls)
        self.db_mock.select_db.assert_called_with('migration_test')
        self.assertEqual(2, self.db_mock.commit.call_count)
        self.assertEqual(5, self.db_mock.close.call_count)

        expected_execute_calls = [
            call('create table if not exists __db_version__ ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default "0", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id))'),
            call('select count(*) from __db_version__;'),
            call('insert into __db_version__ (version) values ("0")'),
            call("select version from __db_version__ where label = 'xxx' order by id desc")
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

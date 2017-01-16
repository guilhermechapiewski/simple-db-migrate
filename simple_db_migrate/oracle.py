import os
import re
import sys

from core import Migration
from core.exceptions import MigrationException
from helpers import Utils
from getpass import getpass
from cli import CLI

class Oracle(object):
    __re_objects = re.compile("(?ims)(?P<pre>.*?)(?P<main>create[ \n\t\r]*(or[ \n\t\r]+replace[ \n\t\r]*)?(trigger|function|procedure|package|package body).*?)\n[ \n\t\r]*/([ \n\t\r]+(?P<pos>.*)|$)")
    __re_anonymous = re.compile("(?ims)(?P<pre>.*?)(?P<main>(declare[ \n\t\r]+.*?)?begin.*?\n[ \n\t\r]*)/([ \n\t\r]+(?P<pos>.*)|$)")

    def __init__(self, config=None, driver=None, get_pass=getpass, std_in=sys.stdin):
        self.__script_encoding = config.get("database_script_encoding", "utf8")
        self.__encoding = config.get("database_encoding", "American_America.UTF8")
        self.__host = config.get("database_host")
        self.__port = config.get("database_port", 1521)
        self.__user = config.get("database_user")
        self.__passwd = config.get("database_password")
        self.__db = config.get("database_name")
        self.__version_table = config.get("database_version_table")

        self.__driver = driver
        if not driver:
            import cx_Oracle
            self.__driver = cx_Oracle

        self.get_pass = get_pass
        self.std_in = std_in

        os.environ["NLS_LANG"] = self.__encoding

        if config.get("drop_db_first"):
            self._drop_database()

        self._create_database_if_not_exists()
        self._create_version_table_if_not_exists()

    def __connect(self):
        try:
            dsn = self.__db
            if self.__host:
                dsn = self.__driver.makedsn(self.__host, self.__port, self.__db)

            return self.__driver.connect(dsn=dsn, user=self.__user, password=self.__passwd)
        except Exception as e:
            raise Exception("could not connect to database: %s" % e)

    def __execute(self, sql, execution_log=None):
        conn = self.__connect()
        cursor = conn.cursor()
        curr_statement = None
        try:
            statments = Oracle._parse_sql_statements(sql)
            if len(sql.strip(' \t\n\r')) != 0 and len(statments) == 0:
                raise Exception("invalid sql syntax '%s'" % sql.encode("utf-8"))

            for statement in statments:
                curr_statement = statement.encode(self.__script_encoding)
                cursor.execute(curr_statement)
                affected_rows = max(cursor.rowcount, 0)
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (curr_statement, affected_rows))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise MigrationException(("error executing migration: %s" % e), curr_statement)

    def __change_db_version(self, version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        params = {}
        params['version'] = version

        conn = self.__connect()
        cursor = conn.cursor()

        if up:
            # moving up and storing history
            sql = "insert into %s (id, version, label, name, sql_up, sql_down) values (%s_seq.nextval, :version, :label, :migration_file_name, :sql_up, :sql_down)" % (self.__version_table, self.__version_table)
            params['sql_up'] = sql_up and sql_up.encode(self.__script_encoding) or ""
            params['sql_down'] = sql_down and sql_down.encode(self.__script_encoding) or ""
            params['migration_file_name'] = migration_file_name
            params['label'] = label_version

            cursor.setinputsizes(sql_up=self.__driver.CLOB, sql_down=self.__driver.CLOB)
        else:
            # moving down and deleting from history
            sql = "delete from %s where version = :version" % (self.__version_table)

        try:
            cursor.execute(sql.encode(self.__script_encoding), params)
            cursor.close()
            conn.commit()
            if execution_log:
                execution_log("migration %s registered\n" % (migration_file_name))
        except Exception as e:
            conn.rollback()
            raise MigrationException(("error logging migration: %s" % e), migration_file_name)
        finally:
            conn.close()

    @classmethod
    def _parse_sql_statements(self, migration_sql):
        all_statements = []
        last_statement = ''

        match_stmt = Oracle.__re_objects.match(migration_sql)
        if not match_stmt:
            match_stmt = Oracle.__re_anonymous.match(migration_sql)

        if match_stmt and match_stmt.re.groups > 0:
            if match_stmt.group('pre'):
                all_statements = all_statements + Oracle._parse_sql_statements(match_stmt.group('pre'))
            if match_stmt.group('main'):
                all_statements.append(match_stmt.group('main'))
            if match_stmt.group('pos'):
                all_statements = all_statements + Oracle._parse_sql_statements(match_stmt.group('pos'))

        else:
            for statement in migration_sql.split(';'):
                if len(last_statement) > 0:
                    curr_statement = '%s;%s' % (last_statement, statement)
                else:
                    curr_statement = statement

                count = Utils.count_occurrences(curr_statement)
                single_quotes = count.get("'", 0)
                double_quotes = count.get('"', 0)
                left_parenthesis = count.get('(', 0)
                right_parenthesis = count.get(')', 0)

                if single_quotes % 2 == 0 and double_quotes % 2 == 0 and left_parenthesis == right_parenthesis:
                    all_statements.append(curr_statement)
                    last_statement = ''
                else:
                    last_statement = curr_statement

        return [s.strip() for s in all_statements if ((s.strip() != "") and (last_statement == ""))]

    def _drop_database(self):
        sql = """\
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
            WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME NOT LIKE 'BIN$%%'""" % (self.__user.upper(), self.__user.upper(), self.__user.upper())

        conn = self.__connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            failed_sqls = ''
            while True:
                row = cursor.fetchone()
                if row is None:
                    break

                drop_sql = row[0]
                try:
                    self.__execute(drop_sql)
                except Exception as e:
                    failed_sqls = failed_sqls + "can't execute drop command '%s' in database '%s', %s\n" % (drop_sql, self.__db, str(e).strip())

            if failed_sqls != '':
                CLI.msg('\nThe following drop commands failed:\n%s' % (failed_sqls), "RED")
                CLI.msg('\nDo you want to continue anyway (y/N):', "END")
                to_continue = self.std_in.readline().strip()
                if to_continue.upper() != 'Y':
                    raise Exception("can't drop database objects for user '%s'" % (self.__user) )

        except Exception as e:
            self._verify_if_exception_is_invalid_user(e)
        finally:
            cursor.close()
            conn.close()


    def _create_database_if_not_exists(self):
        try:
            conn = self.__connect()
            conn.close()
        except Exception as e:
            self._verify_if_exception_is_invalid_user(e)

    def _verify_if_exception_is_invalid_user(self, exception):
        if 'ORA-01017' in exception.__str__():
            CLI.msg('\nPlease inform dba user/password to connect to database "%s"\nUser:' % (self.__host), "END")
            dba_user = self.std_in.readline().strip()
            passwd = self.get_pass()
            conn = self.__driver.connect(dsn=self.__host, user=dba_user, password=passwd)
            cursor = conn.cursor()
            try:
                cursor.execute("create user %s identified by %s" % (self.__user, self.__passwd))
                cursor.execute("grant connect, resource to %s" % (self.__user))
                cursor.execute("grant create public synonym to %s" % (self.__user))
                cursor.execute("grant drop public synonym to %s" % (self.__user))
            except Exception as e:
                raise Exception("check error: %s" % e)
            finally:
                cursor.close()
                conn.close()
        else:
            raise exception

    def _create_version_table_if_not_exists(self):
        # create version table
        try:
            sql = "select version from %s" % self.__version_table
            self.__execute(sql)
        except Exception:
            sql = "create table %s ( id number(11) not null, version varchar2(20) default '0' NOT NULL, label varchar2(255), name varchar2(255), sql_up clob, sql_down clob, CONSTRAINT %s_pk PRIMARY KEY (id) ENABLE)" % (self.__version_table, self.__version_table)
            self.__execute(sql)
            try:
                self.__execute("drop sequence %s_seq" % self.__version_table)
            except:
                pass
            finally:
                self.__execute("create sequence %s_seq start with 1 increment by 1 nomaxvalue" % self.__version_table)

        # check if there is a register there
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select count(*) from %s" % self.__version_table)
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (id, version) values (%s_seq.nextval, '0')" % (self.__version_table, self.__version_table)
            self.__execute(sql)

    def change(self, sql, new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up, execution_log, label_version)

    def get_current_schema_version(self):
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select version from %s order by id desc" % self.__version_table)
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return version

    def get_all_schema_versions(self):
        versions = []
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select version from %s order by id" % self.__version_table)
        while True:
            version = cursor.fetchone()
            if version is None:
                break
            versions.append(version[0])
        cursor.close()
        conn.close()
        versions.sort()
        return versions

    def get_version_id_from_version_number(self, version):
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select id from %s where version = '%s' order by id desc" % (self.__version_table, version))
        result = cursor.fetchone()
        _id = result and int(result[0]) or None
        cursor.close()
        conn.close()
        return _id

    def get_version_number_from_label(self, label):
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select version from %s where label = '%s' order by id desc" % (self.__version_table, label))
        result = cursor.fetchone()
        version = result and result[0] or None
        cursor.close()
        conn.close()
        return version

    def get_all_schema_migrations(self):
        migrations = []
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select id, version, label, name, sql_up, sql_down from %s order by id" % self.__version_table)
        while True:
            migration_db = cursor.fetchone()
            if migration_db is None:
                break

            migration = Migration(id = int(migration_db[0]),
                                  version = migration_db[1] and str(migration_db[1]) or None,
                                  label = migration_db[2] and str(migration_db[2]) or None,
                                  file_name = migration_db[3] and str(migration_db[3]) or None,
                                  sql_up = Migration.ensure_sql_unicode(migration_db[4] and migration_db[4].read() or None, self.__script_encoding),
                                  sql_down = Migration.ensure_sql_unicode(migration_db[5] and migration_db[5].read() or None, self.__script_encoding))
            migrations.append(migration)
        cursor.close()
        conn.close()
        return migrations

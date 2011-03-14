import os
import re
import sys

from core import Migration
from core.exceptions import MigrationException
from helpers import Utils
from getpass import getpass
from cli import CLI

class Oracle(object):

    def __init__(self, config=None, driver=None, get_pass=getpass, std_in=sys.stdin):
        self.__script_encoding = config.get("db_script_encoding", "utf8")
        self.__encoding = config.get("db_encoding", "American_America.UTF8")
        self.__host = config.get("db_host")
        self.__user = config.get("db_user")
        self.__passwd = config.get("db_password")
        self.__db = config.get("db_name")
        self.__version_table = config.get("db_version_table")

        self.__re_objects = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ \n\t\r]*(or[ \n\t\r]+replace[ \n\t\r]*)?(trigger|function|procedure|package|package body).*?)\n[ \n\t\r]*/([ \n\t\r]+(?P<pos>.*)|$)")
        self.__re_anonymous = re.compile("(?ims)(?P<pre>.*?)(?P<principal>(declare[ \n\t\r]+.*?)?begin.*?\n[ \n\t\r]*)/([ \n\t\r]+(?P<pos>.*)|$)")

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
            return self.__driver.connect(dsn=self.__host, user=self.__user, password=self.__passwd)
        except Exception, e:
            raise Exception("could not connect to database: %s" % e)

    def __execute(self, sql, execution_log=None):
        conn = self.__connect()
        cursor = conn.cursor()
        curr_statement = None
        try:
            for statement in self._parse_sql_statements(sql):
                curr_statement = statement
                affected_rows = cursor.execute(statement.encode(self.__script_encoding))
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows and int(affected_rows) or 0))
            cursor.close()
            conn.commit()
            conn.close()
        except Exception, e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise MigrationException("error executing migration: %s" % e, curr_statement)

        return execution_log

    def __change_db_version(self, version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        params = {}
        params['version'] = version

        conn = self.__connect()
        cursor = conn.cursor()

        if up:
            # moving up and storing history
            sql = "insert into %s (id, version, label, name, sql_up, sql_down) values (%s_seq.nextval, :version, :label, :migration_file_name, :sql_up, :sql_down)" % (self.__version_table, self.__version_table)
            sql_up = sql_up and sql_up.encode(self.__script_encoding) or ""
            v_sql_up = cursor.var( self.__driver.CLOB, len(sql_up))
            v_sql_up.setvalue( 0, sql_up )
            params['sql_up'] = sql_up

            sql_down = sql_down and sql_down.encode(self.__script_encoding) or ""
            v_sql_down = cursor.var( self.__driver.CLOB, len(sql_down))
            v_sql_down.setvalue( 0, sql_down )
            params['sql_down'] = sql_down

            params['migration_file_name'] = migration_file_name
            params['label'] = label_version
        else:
            # moving down and deleting from history
            sql = "delete from %s where version = :version" % (self.__version_table)

        try:
            cursor.execute(sql.encode(self.__script_encoding), params)
            if execution_log:
                execution_log("migration %s registered\n" % (migration_file_name))
        except Exception, e:
            raise MigrationException("error logging migration: %s" % e, migration_file_name)
        finally:
            cursor.close()
            conn.commit()
            conn.close()

    def _parse_sql_statements(self, migration_sql):
        all_statements = []
        last_statement = ''

        match_stmt = self.__re_objects.match(migration_sql)
        if not match_stmt:
            match_stmt = self.__re_anonymous.match(migration_sql)

        if match_stmt and match_stmt.re.groups > 0:
            if match_stmt.group('pre'):
                all_statements = all_statements + self._parse_sql_statements(match_stmt.group('pre'))
            if match_stmt.group('principal'):
                all_statements.append(match_stmt.group('principal'))
            if match_stmt.group('pos'):
                all_statements = all_statements + self._parse_sql_statements(match_stmt.group('pos'))

        else:
            for statement in migration_sql.split(';'):
                if len(last_statement) > 0:
                    curr_statement = '%s;%s' % (last_statement, statement)
                else:
                    curr_statement = statement

                single_quotes = Utils.how_many(curr_statement, "'")
                double_quotes = Utils.how_many(curr_statement, '"')
                left_parenthesis = Utils.how_many(curr_statement, '(')
                right_parenthesis = Utils.how_many(curr_statement, ')')

                if single_quotes % 2 == 0 and double_quotes % 2 == 0 and left_parenthesis == right_parenthesis:
                    all_statements.append(curr_statement)
                    last_statement = ''
                else:
                    last_statement = curr_statement

        return [s.strip() for s in all_statements if s.strip() != ""]

    def _drop_database(self):
        try:
            conn = self.__connect()
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
            cursor = conn.cursor()
            resultset = cursor.execute(sql)
            rows = resultset.fetchall()
            cursor.close()
            conn.close()

            failed_sqls = ''
            for row in rows:
                drop_sql = row[0]
                try:
                    self.__execute(drop_sql)
                except Exception, e:
                    failed_sqls = failed_sqls + "can't execute drop command '%s' in database '%s', %s\n" % (drop_sql, self.__db, str(e).strip())

            if failed_sqls != '':
                cli = CLI()
                cli.msg('\nThe following drop commands failed:\n%s' % (failed_sqls), "RED")
                cli.msg('\nDo you want to continue anyway (y/N):', "END")
                to_continue = self.std_in.readline().strip()
                if to_continue.upper() != 'Y':
                    raise Exception("can't drop database '%s'" % (self.__db) )

        except Exception, e:
            self._verify_if_exception_is_invalid_user(e)

    def _create_database_if_not_exists(self):
        try:
            conn = self.__connect()
            conn.close()
        except Exception, e:
            self._verify_if_exception_is_invalid_user(e)

    def _verify_if_exception_is_invalid_user(self, exception):
        if 'ORA-01017' in exception.__str__():
            try:
                cli = CLI()
                cli.msg('\nPlease inform dba user/password to connect to database "%s"\nUser:' % (self.__host), "END")
                dba_user = self.std_in.readline().strip()
                passwd = self.get_pass()
                conn = self.__driver.connect(dsn=self.__host, user=dba_user, password=passwd)
                cursor = conn.cursor()
                cursor.execute("create user %s identified by %s" % (self.__user, self.__passwd))
                cursor.execute("grant connect, resource to %s" % (self.__user))
                cursor.execute("grant create public synonym to %s" % (self.__user))
                cursor.execute("grant drop public synonym to %s" % (self.__user))
                cursor.close()
                conn.close()
            except Exception, e:
                raise Exception("check error: %s" % e)
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
            except Exception:
                pass #nothing to be done here
            finally:
                self.__execute("create sequence %s_seq start with 1 increment by 1 nomaxvalue" % self.__version_table)


        self._check_version_table_if_is_updated()

        # check if there is a register there
        conn = self.__connect()
        cursor = conn.cursor()
        result = cursor.execute("select count(*) from %s" % self.__version_table)
        count = result.fetchone()[0]
        cursor.close()
        conn.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (id, version) values (%s_seq.nextval, '0')" % (self.__version_table, self.__version_table)
            self.__execute(sql)

    def _check_version_table_if_is_updated(self):
        # try to query a column wich not exists on the old version of simple-db-migrate
        # has to have one check of this to each version of simple-db-migrate
        db = self.__connect()
        cursor = db.cursor()
        try:
            cursor.execute("select id from %s" % self.__version_table)
        except Exception, e:
            # update version table
            self.__execute("alter table %s add (id number(11), name varchar2(255), sql_up clob, sql_down clob);" % self.__version_table)
            try:
                self.__execute("drop sequence %s_seq" % self.__version_table)
            except Exception:
                pass #nothing to be done here
            finally:
                sql = """
                create sequence %s_seq start with 1 increment by 1 nomaxvalue;
                update %s set id = %s_seq.nextval;
                alter table %s add constraint db_version_pk primary key (id)
                """ % (self.__version_table, self.__version_table, self.__version_table, self.__version_table)
                self.__execute(sql)

        try:
            cursor.execute("select label from %s" % self.__version_table)
        except Exception, e:
            # update version table
            sql = "alter table %s add (label varchar2(255));" % (self.__version_table)
            self.__execute(sql)

        try:
            cursor.execute("alter table %s drop constraint %s_uk_label" % (self.__version_table, self.__version_table))
        except Exception, e:
            pass

        cursor.close()
        db.close()

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
        all_versions = cursor.fetchall()
        for version in all_versions:
            versions.append(version[0])
        cursor.close()
        conn.close()
        versions.sort()
        return versions

    def get_version_id_from_version_number(self, version):
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select id from %s where version = '%s'" % (self.__version_table, version))
        result = cursor.fetchone()
        id = result and int(result[0]) or None
        cursor.close()
        conn.close()
        return id

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
        all_migrations = cursor.fetchall()
        for migration_db in all_migrations:
            migration = Migration(id = int(migration_db[0]),
                                  version = migration_db[1] and str(migration_db[1]) or None,
                                  label = migration_db[2] and str(migration_db[2]) or None,
                                  file_name = migration_db[3] and str(migration_db[3]) or None,
                                  sql_up = Migration.check_sql_unicode(migration_db[4] and migration_db[4].read() or None, self.__script_encoding),
                                  sql_down = Migration.check_sql_unicode(migration_db[5] and migration_db[5].read() or None, self.__script_encoding))
            migrations.append(migration)
        cursor.close()
        conn.close()
        return migrations

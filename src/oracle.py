import os
import re
import sys

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
        
        self.__re_objects = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?(trigger|function|procedure).*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        
        self.__re_trigger = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?trigger.*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        self.__re_function = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?function.*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        self.__re_procedure = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?procedure.*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        
        self.__re_package = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?trigger.*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        self.__re_package_body = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?trigger.*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        self.__re_anonymous = re.compile("(?ims)(?P<pre>.*?)(?P<principal>create[ ]*(or[ ]+replace[ ]*)?trigger.*?\n[ ]*)/[ \n]+(?P<pos>.*)")
        
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
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows))
            cursor.close()
            conn.commit()
            conn.close()
        except Exception, e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise MigrationException("error executing migration: %s" % e, curr_statement)
        
        return execution_log
            
    def __change_db_version(self, version, up=True):
        if up:
            # moving up and storing history
            sql = "insert into %s (version) values ('%s')" % (self.__version_table, str(version))
        else:
            # moving down and deleting from history
            sql = "delete from %s where version >= '%s'" % (self.__version_table, str(version))
        self.__execute(sql)
        
    def _parse_sql_statements(self, migration_sql):
        all_statements = []
        last_statement = ''
        
        #import pdb; pdb.set_trace()
        match_stmt = self.__re_objects.match(migration_sql)
        #match_stmt = self.__re_trigger.match(migration_sql)
        #if not match_stmt:
        #    match_stmt = self.__re_function.match(migration_sql)
        #if not match_stmt:
        #    match_stmt = self.__re_procedure.match(migration_sql)
        if not match_stmt:
            match_stmt = self.__re_package.match(migration_sql)
        if not match_stmt:
            match_stmt = self.__re_package_body.match(migration_sql)
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
            
            try:
                drop_sqls = "\n".join(["%s" % (row[0]) for row in rows])
                self.__execute(drop_sqls)
            except Exception, e:
                raise Exception("can't drop database '%s'" % self.__db)
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
        except Exception, e:
            sql = "create table %s ( version varchar2(20) default '0' NOT NULL )" % self.__version_table
            self.__execute(sql)
        
        # check if there is a register there
        conn = self.__connect()
        cursor = conn.cursor()
        result = cursor.execute("select count(*) from %s" % self.__version_table)
        count = result.fetchone()[0]
        cursor.close()
        conn.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (version) values ('0')" % self.__version_table
            self.__execute(sql)
    
    def change(self, sql, new_db_version, up=True, execution_log=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, up)
        
    def get_current_schema_version(self):
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select version from %s order by version desc" % self.__version_table)
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return version
    
    def get_all_schema_versions(self):
        versions = []
        conn = self.__connect()
        cursor = conn.cursor()
        cursor.execute("select version from %s order by version" % self.__version_table)
        all_versions = cursor.fetchall()
        for version in all_versions:
            versions.append(version[0])
        cursor.close()
        conn.close()
        versions.sort()
        return versions

import re
import sys

import MySQLdb

from core.exceptions import MigrationException
from helpers import Utils

class MySQL(object):
    
    def __init__(self, config=None, mysql_driver=MySQLdb):
        self.__mysql_driver = mysql_driver
        self.__mysql_host = config.get("db_host")
        self.__mysql_user = config.get("db_user")
        self.__mysql_passwd = config.get("db_password")
        self.__mysql_db = config.get("db_name")
        self.__version_table = config.get("db_version_table")

        if config.get("drop_db_first"):
            self._drop_database()
            
        self._create_database_if_not_exists()
        self._create_version_table_if_not_exists()

    def __mysql_connect(self, connect_using_db_name=True):
        try:
            conn = self.__mysql_driver.connect(host=self.__mysql_host, user=self.__mysql_user, passwd=self.__mysql_passwd)

            # this should be configured in the config file, not hardcoded
            conn.set_character_set('utf8')
            
            if connect_using_db_name:
                conn.select_db(self.__mysql_db)
            return conn
        except Exception, e:
            raise Exception("could not connect to database: %s" % e)
    
    def __execute(self, sql, execution_log=None):
        db = self.__mysql_connect()        
        cursor = db.cursor()
        cursor._defer_warnings = True
        curr_statement = None
        try:
            for statement in self._parse_sql_statements(sql):
                curr_statement = statement
                affected_rows = cursor.execute(statement.encode("utf-8"))
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows))
            cursor.close()        
            db.commit()
            db.close()
        except Exception, e:
            raise MigrationException("error executing migration: %s" % e, curr_statement)
        
        return execution_log
            
    def __change_db_version(self, version, up=True):
        if up:
            # moving up and storing history
            sql = "insert into %s (version) values (\"%s\");" % (self.__version_table, str(version))
        else:
            # moving down and deleting from history
            sql = "delete from %s where version >= \"%s\";" % (self.__version_table, str(version))
        self.__execute(sql)
        
    def _parse_sql_statements(self, migration_sql):
        all_statements = []
        last_statement = ''
        
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
        db = self.__mysql_connect(False)
        try:
            db.query("set foreign_key_checks=0; drop database if exists %s;" % self.__mysql_db)
        except Exception, e:
            raise Exception("can't drop database '%s'; database doesn't exist" % self.__mysql_db)
        db.close()
        
    def _create_database_if_not_exists(self):
        db = self.__mysql_connect(False)
        db.query("create database if not exists %s;" % self.__mysql_db)
        db.close()
    
    def _create_version_table_if_not_exists(self):
        # create version table
        sql = "create table if not exists %s ( version varchar(20) NOT NULL default \"0\" );" % self.__version_table
        self.__execute(sql)
        
        # check if there is a register there
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select count(*) from %s;" % self.__version_table)
        count = cursor.fetchone()[0]
        db.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (version) values (\"0\");" % self.__version_table
            self.__execute(sql)
    
    def change(self, sql, new_db_version, up=True, execution_log=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, up)
        
    def get_current_schema_version(self):
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s order by version desc limit 0,1;" % self.__version_table)
        version = cursor.fetchone()[0]
        db.close()
        return version
    
    def get_all_schema_versions(self):
        versions = []
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s order by version;" % self.__version_table)
        all_versions = cursor.fetchall()
        for version in all_versions:
            versions.append(version[0])
        db.close()
        versions.sort()
        return versions

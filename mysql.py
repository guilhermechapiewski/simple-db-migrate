from logging import *
import MySQLdb
import sys

class MySQL(object):
    
    def __init__(self, db_config_file):
        # read configurations
        f = None
        try:
            f = open(db_config_file, "r")
        except IOError:
            Log().error_and_exit("%s: file not found" % db_config_file)
        
        exec(f.read())
        f.close()
        
        self.__mysql_host__ = HOST
        self.__mysql_user__ = USERNAME
        self.__mysql_passwd__ = PASSWORD
        self.__mysql_db__ = DATABASE
                
        self.__create_database_if_not_exists()
        self.__create_version_table_if_not_exists()

    def __mysql_connect(self, connect_using_db_name=True):
        if connect_using_db_name:
            return MySQLdb.connect(host=self.__mysql_host__, user=self.__mysql_user__, passwd=self.__mysql_passwd__, db=self.__mysql_db__)
        
        return MySQLdb.connect(host=self.__mysql_host__, user=self.__mysql_user__, passwd=self.__mysql_passwd__)
    
    def __execute(self, sql):
        db = self.__mysql_connect()
        db.query(sql)
        db.close()
        
    def __create_database_if_not_exists(self):
        db = self.__mysql_connect(False)
        db.query("create database if not exists %s;" % self.__mysql_db__)
        db.close()
    
    def __create_version_table_if_not_exists(self):
        # create version table
        sql = "create table if not exists __db_version__ ( version int(11) NOT NULL default 0 )"
        self.__execute(sql)
        
        # check if there is a register there
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select count(*) from __db_version__")
        count = cursor.fetchone()[0]
        db.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into __db_version__ values (0)"
            self.__execute(sql)
    
    def __set_new_db_version(self, version):
        sql = "update __db_version__ set version = %s" % str(version)
        self.__execute(sql)
    
    def change(self, sql, new_db_version):
        self.__execute(sql)
        self.__set_new_db_version(new_db_version)
        
    def get_current_db_version(self):
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select * from __db_version__")
        version = cursor.fetchone()[0]
        db.close()
        return version


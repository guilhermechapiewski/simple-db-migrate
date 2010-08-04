# coding: utf-8

from core.exceptions import MigrationException

class DbTier(object):
    def __init__(self, config, db_driver):
        """initializes a new DbTier"""
        self.config = config
        self.db_driver = db_driver

    #orchestrators
    def initialize_db(self):
        if self.config.get("drop_db_first"):
            self.drop_db()
        self.create_db()

    def verify_db_consistency(self):
        self.create_primary_key_in_versions_table()

    #executers

    def create_db(self):
        db_name = self.config.get("db_name")
        sql = "create database if not exists %s;" % db_name

        self.db_driver.execute(sql)

    def drop_db(self):
        try:
            db_name = self.config.get("db_name")
            sql = "set foreign_key_checks=0; drop database if exists %s;" % db_name
            self.db_driver.execute(sql)
        except MigrationException, e:
            raise MigrationException("can't drop database '%s'; database doesn't exist" % db_name)

    def create_version_table(self):
        version_table = self.config.get("db_version_table")
        sql = "create table if not exists %s ( version varchar(20) NOT NULL default \"0\" );" % version_table
        self.db_driver.execute(sql)

    def verify_if_migration_zero_is_present(self):
        version_table = self.config.get("db_version_table")
        sql = "select count(*) from %s;" % version_table
        count = self.db_driver.query_scalar(sql)

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (version) values (\"0\");" % version_table
            self.db_driver.execute(sql)

    def create_primary_key_in_versions_table(self):
        try:
            sql = "alter table version add id int(11) primary key auto_increment not null;"
            self.db_driver.execute(sql)
        except MigrationException:
            pass

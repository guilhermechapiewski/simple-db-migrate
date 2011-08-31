from core import Migration
from core.exceptions import MigrationException
from helpers import Utils

class MySQL(object):

    def __init__(self, config=None, mysql_driver=None):
        self.__mysql_script_encoding = config.get("db_script_encoding", "utf8")
        self.__mysql_encoding = config.get("db_encoding", "utf8")
        self.__mysql_host = config.get("db_host")
        self.__mysql_user = config.get("db_user")
        self.__mysql_passwd = config.get("db_password")
        self.__mysql_db = config.get("db_name")
        self.__version_table = config.get("db_version_table")

        self.__mysql_driver = mysql_driver
        if not mysql_driver:
            import MySQLdb
            self.__mysql_driver = MySQLdb

        if config.get("drop_db_first"):
            self._drop_database()

        self._create_database_if_not_exists()
        self._create_version_table_if_not_exists()

    def __mysql_connect(self, connect_using_db_name=True):
        try:
            conn = self.__mysql_driver.connect(host=self.__mysql_host, user=self.__mysql_user, passwd=self.__mysql_passwd)

            conn.set_character_set(self.__mysql_encoding)

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
                affected_rows = cursor.execute(statement.encode(self.__mysql_script_encoding))
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows and int(affected_rows) or 0))
            cursor.close()
            db.commit()
            db.close()
        except Exception, e:
            raise MigrationException("error executing migration: %s" % e, curr_statement)

        return execution_log

    def __change_db_version(self, version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        if up:
            if not label_version:
                label_version = "NULL"
            else:
                label_version = "\"%s\"" % (str(label_version))
            # moving up and storing history
            sql = "insert into %s (version, label, name, sql_up, sql_down) values (\"%s\", %s, \"%s\", \"%s\", \"%s\");" % (self.__version_table, str(version), label_version, migration_file_name, sql_up.replace('"', '\\"'), sql_down.replace('"', '\\"'))
        else:
            # moving down and deleting from history
            sql = "delete from %s where version = \"%s\";" % (self.__version_table, str(version))

        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor._defer_warnings = True
        try:
            cursor.execute(sql.encode(self.__mysql_script_encoding))
            if execution_log:
                execution_log("migration %s registered\n" % (migration_file_name))
        except Exception, e:
            raise MigrationException("error logging migration: %s" % e, migration_file_name)
        finally:
            cursor.close()
            db.commit()
            db.close()

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
            db.query("set foreign_key_checks=0; drop database if exists `%s`;" % self.__mysql_db)
        except Exception:
            raise Exception("can't drop database '%s'; database doesn't exist" % self.__mysql_db)
        db.close()

    def _create_database_if_not_exists(self):
        db = self.__mysql_connect(False)
        db.query("create database if not exists `%s`;" % self.__mysql_db)
        db.close()

    def _create_version_table_if_not_exists(self):
        # create version table
        sql = "create table if not exists %s ( id int(11) NOT NULL AUTO_INCREMENT, version varchar(20) NOT NULL default \"0\", label varchar(255), name varchar(255), sql_up LONGTEXT, sql_down LONGTEXT, PRIMARY KEY (id));" % self.__version_table
        self.__execute(sql)

        self._check_version_table_if_is_updated()

        # check if there is a register there
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select count(*) from %s;" % self.__version_table)
        count = cursor.fetchone()[0]
        cursor.close()
        db.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (version) values (\"0\");" % self.__version_table
            self.__execute(sql)

    def _check_version_table_if_is_updated(self):
        # try to query a column wich not exists on the old version of simple-db-migrate
        # has to have one check of this to each version of simple-db-migrate
        db = self.__mysql_connect()
        cursor = db.cursor()
        try:
            cursor.execute("select id from %s;" % self.__version_table)
        except Exception:
            # update version table
            sql = "alter table %s add column id int(11)  not null auto_increment first, add column name varchar(255), add column sql_up longtext, add column sql_down longtext, add primary key (id);" % self.__version_table
            self.__execute(sql)

        try:
            cursor.execute("select label from %s;" % self.__version_table)
        except Exception:
            # update version table
            sql = "alter table %s add column label varchar(255) after version;" % self.__version_table
            self.__execute(sql)

        try:
            cursor.execute("show index from %s where key_name = 'label';" % self.__version_table)
            if cursor.fetchone():
                cursor.execute("alter table %s drop index label;" % self.__version_table)
        except Exception:
            pass

        cursor.close()
        db.close()

    def change(self, sql, new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up, execution_log, label_version)

    def get_current_schema_version(self):
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s order by id desc limit 0,1;" % self.__version_table)
        version = cursor.fetchone()[0]
        cursor.close()
        db.close()
        return version

    def get_all_schema_versions(self):
        versions = []
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s order by id;" % self.__version_table)
        all_versions = cursor.fetchall()
        for version in all_versions:
            versions.append(version[0])
        cursor.close()
        db.close()
        versions.sort()
        return versions

    def get_version_id_from_version_number(self, version):
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select id from %s where version = '%s';" % (self.__version_table, version))
        result = cursor.fetchone()
        id = result and int(result[0]) or None
        cursor.close()
        db.close()
        return id

    def get_version_number_from_label(self, label):
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s where label = '%s' order by id desc" % (self.__version_table, label))
        result = cursor.fetchone()
        version = result and result[0] or None
        cursor.close()
        db.close()
        return version

    def get_all_schema_migrations(self):
        migrations = []
        db = self.__mysql_connect()
        cursor = db.cursor()
        cursor.execute("select id, version, label, name, sql_up, sql_down from %s order by id;" % self.__version_table)
        all_migrations = cursor.fetchall()
        for migration_db in all_migrations:
            migration = Migration(id = int(migration_db[0]),
                                  version = migration_db[1] and str(migration_db[1]) or None,
                                  label = migration_db[2] and str(migration_db[2]) or None,
                                  file_name = migration_db[3] and str(migration_db[3]) or None,
                                  sql_up = Migration.check_sql_unicode(migration_db[4], self.__mysql_script_encoding),
                                  sql_down = Migration.check_sql_unicode(migration_db[5], self.__mysql_script_encoding))
            migrations.append(migration)
        cursor.close()
        db.close()
        return migrations

from core import Migration
from core.exceptions import MigrationException
from helpers import Utils

class PostgreSQL(object):

    def __init__(self, config=None, postgresql_driver=None):
        self.__postgresql_script_encoding = config.get("database_script_encoding", "utf8")
        self.__postgresql_encoding = config.get("database_encoding", "utf8")
        self.__postgresql_host = config.get("database_host")
        self.__postgresql_port = config.get("database_port", 5432)
        self.__postgresql_user = config.get("database_user")
        self.__postgresql_passwd = config.get("database_password")
        self.__postgresql_db = config.get("database_name")
        self.__version_table = config.get("database_version_table")

        self.__postgresql_driver = postgresql_driver
        if not postgresql_driver:
            import psycopg2
            self.__postgresql_driver = psycopg2

        if config.get("drop_db_first"):
            self._drop_database()

        self._create_database_if_not_exists()
        self._create_version_table_if_not_exists()

    def __postgresql_connect(self, connect_using_database_name=True):
        try:
            if connect_using_database_name:
                conn = self.__postgresql_driver.connect(database=self.__postgresql_db, user=self.__postgresql_user, password=self.__postgresql_passwd, host=self.__postgresql_host, port=self.__postgresql_port)
            else:
                conn = self.__postgresql_driver.connect(user=self.__postgresql_user, password=self.__postgresql_passwd, host=self.__postgresql_host, port=self.__postgresql_port)

            conn.set_isolation_level(0)
            #conn.set_character_set(self.__postgresql_encoding)
            conn.set_client_encoding(self.__postgresql_encoding)

            return conn
        except Exception, e:
            raise Exception("could not connect to database: %s" % e)

    def __execute(self, sql, execution_log=None):
        db = self.__postgresql_connect()
        cursor = db.cursor()
        #cursor._defer_warnings = True
        curr_statement = None
        try:
            statments = PostgreSQL._parse_sql_statements(sql)
            if len(sql.strip(' \t\n\r')) != 0 and len(statments) == 0:
                raise Exception("invalid sql syntax '%s'" % sql)

            for statement in statments:
                curr_statement = statement
                affected_rows = cursor.execute(statement.encode(self.__postgresql_script_encoding))
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows and int(affected_rows) or 0))
            cursor.close()
            db.commit()
        except Exception, e:
            db.rollback()
            raise MigrationException("error executing migration: %s" % e, curr_statement)
        finally:
            db.close()

    def __change_db_version(self, version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        if up:
            if not label_version:
                label_version = "NULL"
            else:
                label_version = "\'%s\'" % (str(label_version))
            # moving up and storing history
            sql = "insert into %s (version, label, name, sql_up, sql_down) values (\'%s\', %s, \'%s\', \'%s\', \'%s\');" % (self.__version_table, str(version), label_version, migration_file_name, sql_up.replace('"', '\\"').replace("'","''"), sql_down.replace('"', '\\"').replace("'","''"))
        else:
            # moving down and deleting from history
            sql = "delete from %s where version = \'%s\';" % (self.__version_table, str(version))

        db = self.__postgresql_connect()
        cursor = db.cursor()
        #cursor._defer_warnings = True
        try:
            cursor.execute(sql.encode(self.__postgresql_script_encoding))
            cursor.close()
            db.commit()
            if execution_log:
                execution_log("migration %s registered\n" % migration_file_name)
        except Exception, e:
            db.rollback()
            raise MigrationException("error logging migration: %s" % e, migration_file_name)
        finally:
            db.close()

    @classmethod
    def _parse_sql_statements(cls, migration_sql):
        all_statements = []
        last_statement = ''

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
        connection = self.__postgresql_connect(False)
        try:
            cursor = connection.cursor()
            cursor.query("alter table %s disable trigger all; drop database if exists %s;" % (self.__postgresql_db, self.__postgresql_db))
            cursor.close()
        except Exception, e:
            raise Exception("can't drop database '%s'; \n%s" % (self.__postgresql_db, str(e)))
        finally:
            connection.close()

    def _create_database_if_not_exists(self):
        connection = self.__postgresql_connect(False)
        cursor = connection.cursor()

        try:
            cursor.execute("create database %s;" % self.__postgresql_db)
        except Exception:
            pass
        finally:
            cursor.close()
            connection.close()

    def _create_version_table_if_not_exists(self):
        # create version table
        sql = "create table if not exists %s ( id SERIAL PRIMARY KEY, version varchar(20) NOT NULL default \'0\', label varchar(255), name varchar(255), sql_up TEXT, sql_down TEXT);" % self.__version_table
        self.__execute(sql)

        # check if there is a register there
        connection = self.__postgresql_connect()
        cursor = connection.cursor()
        cursor.execute("select count(*) from %s;" % self.__version_table)
        count = cursor.fetchone()[0]
        cursor.close()
        connection.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (version) values (\'0\');" % self.__version_table
            self.__execute(sql)

    def change(self, sql, new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up, execution_log, label_version)

    def get_current_schema_version(self):
        db = self.__postgresql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s order by id desc offset 0 limit 1;" % self.__version_table)
        version = cursor.fetchone()[0]
        cursor.close()
        db.close()
        return version

    def get_all_schema_versions(self):
        versions = []
        db = self.__postgresql_connect()
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
        db = self.__postgresql_connect()
        cursor = db.cursor()
        cursor.execute("select id from %s where version = '%s' order by id desc;" % (self.__version_table, version))
        result = cursor.fetchone()
        _id = result and int(result[0]) or None
        cursor.close()
        db.close()
        return _id

    def get_version_number_from_label(self, label):
        db = self.__postgresql_connect()
        cursor = db.cursor()
        cursor.execute("select version from %s where label = '%s' order by id desc" % (self.__version_table, label))
        result = cursor.fetchone()
        version = result and result[0] or None
        cursor.close()
        db.close()
        return version

    def get_all_schema_migrations(self):
        migrations = []
        db = self.__postgresql_connect()
        cursor = db.cursor()
        cursor.execute("select id, version, label, name, sql_up, sql_down from %s order by id;" % self.__version_table)
        all_migrations = cursor.fetchall()
        for migration_db in all_migrations:
            migration = Migration(id = int(migration_db[0]),
                                  version = migration_db[1] and str(migration_db[1]) or None,
                                  label = migration_db[2] and str(migration_db[2]) or None,
                                  file_name = migration_db[3] and str(migration_db[3]) or None,
                                  sql_up = Migration.ensure_sql_unicode(migration_db[4], self.__postgresql_script_encoding),
                                  sql_down = Migration.ensure_sql_unicode(migration_db[5], self.__postgresql_script_encoding))
            migrations.append(migration)
        cursor.close()
        db.close()
        return migrations

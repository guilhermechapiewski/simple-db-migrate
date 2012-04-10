from core import Migration
from core.exceptions import MigrationException
from helpers import Utils

class MSSQL(object):

    def __init__(self, config=None, mssql_driver=None):
        self.__mssql_script_encoding = config.get("db_script_encoding", "utf8")
        self.__mssql_encoding = config.get("db_encoding", "utf8")
        self.__mssql_host = config.get("db_host")
        self.__mssql_user = config.get("db_user")
        self.__mssql_passwd = config.get("db_password")
        self.__mssql_db = config.get("db_name")
        self.__version_table = config.get("db_version_table")

        self.__mssql_driver = mssql_driver
        if not mssql_driver:
            import _mssql
            self.__mssql_driver = _mssql

        if config.get("drop_db_first"):
            self._drop_database()

        self._create_database_if_not_exists()
        self._create_version_table_if_not_exists()

    def __mssql_connect(self, connect_using_db_name=True):
        try:
            conn = self.__mssql_driver.connect(server=self.__mssql_host, user=self.__mssql_user, password=self.__mssql_passwd, charset=self.__mssql_encoding)
            if connect_using_db_name:
                conn.select_db(self.__mssql_db)
            return conn
        except Exception, e:
            raise Exception("could not connect to database: %s" % e)

    def __execute(self, sql, execution_log=None):
        db = self.__mssql_connect()
        curr_statement = None
        try:
            for statement in MSSQL._parse_sql_statements(sql):
                curr_statement = statement
                db.execute_non_query(statement)
                affected_rows = db.rows_affected
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows and int(affected_rows) or 0))
        except Exception, e:
            db.cancel()
            raise MigrationException("error executing migration: %s" % e, curr_statement)
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

        return [s.strip() for s in all_statements if s.strip() != ""]

    def _drop_database(self):
        db = self.__mssql_connect(False)
        try:
            db.execute_non_query("if exists ( select 1 from sysdatabases where name = '%s' ) drop database %s;" % (self.__mssql_db, self.__mssql_db))
        except Exception, e:
            raise Exception("can't drop database '%s'; \n%s" % (self.__mssql_db, str(e)))
        finally:
            db.close()

    def _create_database_if_not_exists(self):
        db = self.__mssql_connect(False)
        db.execute_non_query("if not exists ( select 1 from sysdatabases where name = '%s' ) create database %s;" % (self.__mssql_db, self.__mssql_db))
        db.close()

    def _create_version_table_if_not_exists(self):
        # create version table
        sql = "if not exists ( select 1 from sysobjects where name = '%s' and type = 'u' ) create table %s ( id INT IDENTITY(1,1) NOT NULL PRIMARY KEY, version varchar(20) NOT NULL default '0', label varchar(255), name varchar(255), sql_up NTEXT, sql_down NTEXT);" % (self.__version_table, self.__version_table)
        self.__execute(sql)

        self._check_version_table_if_is_updated()

        # check if there is a register there
        db = self.__mssql_connect()
        count = db.execute_scalar("select count(*) from %s;" % self.__version_table)
        db.close()

        # if there is not a version register, insert one
        if count == 0:
            sql = "insert into %s (version) values ('0');" % self.__version_table
            self.__execute(sql)

    def _check_version_table_if_is_updated(self):
        # try to query a column wich not exists on the old version of simple-db-migrate
        # has to have one check of this to each version of simple-db-migrate
        db = self.__mssql_connect()
        try:
            db.execute_non_query("select id from %s;" % self.__version_table)
        except Exception:
            # update version table
            sql = "alter table %s add id INT IDENTITY(1,1) NOT NULL PRIMARY KEY, label varchar(255), name varchar(255), sql_up ntext, sql_down ntext;" % self.__version_table
            self.__execute(sql)

        db.close()

    def __change_db_version(self, version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        params = []
        params.append(version)

        if up:
            # moving up and storing history
            sql = "insert into %s (version, label, name, sql_up, sql_down) values (%%s, %%s, %%s, %%s, %%s);" % (self.__version_table)
            params.append(label_version)
            params.append(migration_file_name)
            params.append(sql_up and sql_up.encode(self.__mssql_script_encoding) or "")
            params.append(sql_down and sql_down.encode(self.__mssql_script_encoding) or "")
        else:
            # moving down and deleting from history
            sql = "delete from %s where version = %%s;" % (self.__version_table)

        db = self.__mssql_connect()
        try:
            db.execute_non_query(sql.encode(self.__mssql_script_encoding), tuple(params))
            if execution_log:
                execution_log("migration %s registered\n" % (migration_file_name))
        except Exception, e:
            db.cancel()
            raise MigrationException("error logging migration: %s" % e, migration_file_name)
        finally:
            db.close()

    def change(self, sql, new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up, execution_log, label_version)

    def get_current_schema_version(self):
        db = self.__mssql_connect()
        version = db.execute_scalar("select top 1 version from %s order by id desc" % self.__version_table) or 0
        db.close()
        return version

    def get_all_schema_versions(self):
        versions = []
        db = self.__mssql_connect()
        db.execute_query("select version from %s order by id;" % self.__version_table)
        all_versions = db
        for version in all_versions:
            versions.append(version['version'])
        db.close()
        versions.sort()
        return versions

    def get_version_id_from_version_number(self, version):
        db = self.__mssql_connect()
        result = db.execute_row("select id from %s where version = '%s';" % (self.__version_table, version))
        id = result and int(result['id']) or None
        db.close()
        return id

    def get_version_number_from_label(self, label):
        db = self.__mssql_connect()
        result = db.execute_row("select version from %s where label = '%s' order by id desc" % (self.__version_table, label))
        version = result and result['version'] or None
        db.close()
        return version

    def get_all_schema_migrations(self):
        migrations = []
        db = self.__mssql_connect()
        db.execute_query("select id, version, label, name, cast(sql_up as text) as sql_up, cast(sql_down as text) as sql_down from %s order by id;" % self.__version_table)
        all_migrations = db
        for migration_db in all_migrations:
            migration = Migration(id = int(migration_db['id']),
                                  version = migration_db['version'] and str(migration_db['version']) or None,
                                  label = migration_db['label'] and str(migration_db['label']) or None,
                                  file_name = migration_db['name'] and str(migration_db['name']) or None,
                                  sql_up = Migration.ensure_sql_unicode(migration_db['sql_up'], self.__mssql_script_encoding),
                                  sql_down = Migration.ensure_sql_unicode(migration_db['sql_down'], self.__mssql_script_encoding))
            migrations.append(migration)
        db.close()
        return migrations

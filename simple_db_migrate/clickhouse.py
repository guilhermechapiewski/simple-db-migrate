import re
from .core import Migration
from .core.exceptions import MigrationException
from .helpers import Utils
from clickhouse_driver.client import Client

class ClickHouse(object):
    __re_objects = re.compile("(?ims)(?P<pre>.*?)(?P<main>create[ \n\t\r]*(definer[ \n\t\r]*=[ \n\t\r]*[^ \n\t\r]*[ \n\t\r]*)?(trigger|function|procedure).*?)\n[ \n\t\r]*/([ \n\t\r]+(?P<pos>.*)|$)")

    def __init__(self, config=None):
        self.__clickhouse_script_encoding = config.get("database_script_encoding", "utf8")
        self.__clickhouse_encoding = config.get("database_encoding", "utf8")
        self.__clickhouse_host = config.get("database_host")
        self.__clickhouse_port = config.get("database_port", 9000)
        self.__clickhouse_user = config.get("database_user")
        self.__clickhouse_passwd = config.get("database_password")
        self.__clickhouse_db = config.get("database_name")
        self.__version_table = config.get("database_version_table")

        if config.get("drop_db_first"):
            self._drop_database()
        self.__conn = Client(
                self.__clickhouse_host,
                self.__clickhouse_port,
                self.__clickhouse_db,
                self.__clickhouse_user,
                self.__clickhouse_passwd)
        self._create_database_if_not_exists()
        self._create_version_table_if_not_exists()

    def __clickhouse_connect(self, connect_using_database_name=True):
        try:
            return self.__conn
        except Exception as e:
            raise Exception("could not connect to database: %s" % e)

    def __execute(self, sql, execution_log=None):
        db = self.__clickhouse_connect()
        try:
            statements = ClickHouse._parse_sql_statements(sql)
            if len(sql.strip(' \t\n\r')) != 0 and len(statements) == 0:
                raise Exception("invalid sql syntax '%s'" % Utils.encode(sql, "utf-8"))

            for statement in statements:
                curr_statement = statement
                affected_rows = db.execute(Utils.encode(statement, self.__clickhouse_script_encoding))
                if execution_log:
                    execution_log("%s\n-- %d row(s) affected\n" % (statement, affected_rows and len(affected_rows) or 0))
                return affected_rows
        except Exception as e:
            raise MigrationException("error executing migration: %s" % e, curr_statement)

    def __change_db_version(self, version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        db = self.__clickhouse_connect()
        id = 1
        rows = db.execute("select max(id) from %s; " % self.__version_table)
        if rows and len(rows) > 0:
            id = rows[0][0] + 1
        if up:
            if not label_version:
                label_version = "NULL"
            else:
                label_version = "\"%s\"" % (str(label_version))
            # moving up and storing history
            data = [[id, str(version), label_version, migration_file_name, sql_up.replace('"', '\\"'), sql_down.replace('"', '\\"')]]
            db.process_insert_query("INSERT INTO %s (id, version, label, name, sql_up, sql_down) VALUES " % self.__version_table, data)
        else:
            # moving down and deleting from history
            rows = db.execute("select id from %s_active WHERE version = '%s' ORDER BY id DESC LIMIT 1;" % (self.__version_table, version))
            if rows and len(rows) > 0:
                id = rows[0][0]
                #sql = "delete from %s where version = \"%s\";" % (self.__version_table, str(version))
                sql = "INSERT INTO %s_deleted (id) VALUES " % (self.__version_table,)
                db.process_insert_query(sql, [[id]])
            else:
                raise Exception("Version %s didn't found" % version)

    @classmethod
    def _parse_sql_statements(cls, migration_sql):
        all_statements = []
        last_statement = ''

        match_stmt = ClickHouse.__re_objects.match(migration_sql)

        if match_stmt and match_stmt.re.groups > 0:
            if match_stmt.group('pre'):
                all_statements = all_statements + ClickHouse._parse_sql_statements(match_stmt.group('pre'))
            if match_stmt.group('main'):
                all_statements.append(match_stmt.group('main'))
            if match_stmt.group('pos'):
                all_statements = all_statements + ClickHouse._parse_sql_statements(match_stmt.group('pos'))

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
        try:
            self.__execute("DROP DATABASE IF EXISTS %s" % (self.__clickhouse_db,))
        except Exception as e:
            raise Exception("can't drop database '%s'; \n%s" % (self.__clickhouse_db, str(e)))

    def _create_database_if_not_exists(self):
        self.__execute("CREATE DATABASE IF NOT EXISTS %s" % (self.__clickhouse_db,))
        self.__execute("USE %s" % (self.__clickhouse_db,))

    def _create_version_table_if_not_exists(self):
        # create version table
        sql = "CREATE TABLE IF NOT EXISTS %s ( id Int32, version String, label String, name String, sql_up String, sql_down String, install_delete Int32, date_time DateTime DEFAULT now()) ENGINE = Log " % self.__version_table
        self.__execute(sql)
        sql = "CREATE TABLE IF NOT EXISTS %s_deleted ( id Int32, date_time DateTime DEFAULT now()) ENGINE = Log " % self.__version_table
        self.__execute(sql)
        sql = "CREATE VIEW IF NOT EXISTS %s_active AS SELECT * FROM db_migrations where not (id IN (select id from %s_deleted ))" % (self.__version_table, self.__version_table,)
        self.__execute(sql)

    def change(self, sql, new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None):
        self.__execute(sql, execution_log)
        self.__change_db_version(new_db_version, migration_file_name, sql_up, sql_down, up, execution_log, label_version)

    def get_current_schema_version(self):
        db = self.__clickhouse_connect()
        rows = db.execute("SELECT version FROM %s_active order by id desc limit 0,1;" % (self.__version_table,))
        version = ""
        if rows and len(rows) > 0:
            version = rows[0][0]
        return version

    def get_all_schema_versions(self):
        versions = []
        rows = self.__execute("SELECT version FROM %s_active order by id;" % self.__version_table)
        for row in rows:
            versions.append(row[0])
        versions.sort()
        return versions

    def get_version_id_from_version_number(self, version):
        rows = self.__execute("SELECT id FROM %s_active where version = '%s' order by id desc;" % (self.__version_table, version))
        _id = None
        if len(rows) > 0:
            _id = rows[0][0]
        return _id

    def get_version_number_from_label(self, label):
        rows = self.__execute("SELECT version FROM %s_active where label = '%s' order by id desc" % (self.__version_table, label))
        version = ""
        if len(rows) > 0:
            version = rows[0][0]
        return version

    def get_all_schema_migrations(self):
        migrations = []
        all_migrations = self.__execute("SELECT id, version, label, name, sql_up, sql_down FROM %s_active order by id;" % self.__version_table)
        for migration_db in all_migrations:
            migration = Migration(id = int(migration_db[0]),
                                  version = migration_db[1] and str(migration_db[1]) or None,
                                  label = migration_db[2] and str(migration_db[2]) or None,
                                  file_name = migration_db[3] and str(migration_db[3]) or None,
                                  sql_up = Migration.ensure_sql_unicode(migration_db[4], self.__clickhouse_script_encoding),
                                  sql_down = Migration.ensure_sql_unicode(migration_db[5], self.__clickhouse_script_encoding))
            migrations.append(migration)
        return migrations

from cli import CLI
from log import LOG
from core import Migration, SimpleDBMigrate
from helpers import Lists
from config import Config

"""
The sgbd class should implement the following methods
- change(self, sql, new_db_version, migration_file_name, sql_up, sql_down, up=True, execution_log=None, label_version=None)
  executes the migration (up or down) and records the change on version table
- get_all_schema_migrations(self)
  return all migrations saved on version table
- get_all_schema_versions(self)
  return all versions saved on version table
- get_current_schema_version(self)
  return the current schema version
- get_version_id_from_version_number(self, version)
  return the id from an specific version
- get_version_number_from_label(self, label)
  return the version of the last migration executed under the label
"""

class Main(object):

    def __init__(self, config, sgdb=None):
        Main._check_configuration(config)

        self.cli = CLI()
        self.config = config
        self.log = LOG(self.config.get("log_dir", None))

        self.sgdb = sgdb
        if self.sgdb is None and not self.config.get("new_migration", None):
            if self.config.get("database_engine") == 'mysql':
                from mysql import MySQL
                self.sgdb = MySQL(config)
            elif self.config.get("database_engine") == 'oracle':
                from oracle import Oracle
                self.sgdb = Oracle(config)
            elif self.config.get("database_engine") == 'mssql':
                from mssql import MSSQL
                self.sgdb = MSSQL(config)
            else:
                raise Exception("engine not supported '%s'" % self.config.get("database_engine"))

        self.db_migrate = SimpleDBMigrate(self.config)

    def execute(self):
        self._execution_log('\nStarting DB migration on host/database "%s/%s" with user "%s"...' % (self.config.get('database_host'), self.config.get('database_name'), self.config.get('database_user')), "PINK", log_level_limit=1)        
        if self.config.get("new_migration", None):
            self._create_migration()
        else:
            self._migrate()
        self._execution_log("\nDone.\n", "PINK", log_level_limit=1)

    @staticmethod
    def _check_configuration(config):
        if not isinstance(config, Config):
            raise Exception("config must be an instance of simple_db_migrate.config.Config")

        required_configs = ['database_host', 'database_name', 'database_user', 'database_password', 'database_migrations_dir', 'database_engine', 'schema_version']
        if config.get("new_migration", None):
            required_configs = ['database_migrations_dir']

        for key in required_configs:
            #check if config has the key, if do not have will raise exception
            config.get(key)

    def _create_migration(self):
        migrations_dir = self.config.get("database_migrations_dir")
        new_file = Migration.create(self.config.get("new_migration", None), migrations_dir[0], self.config.get("database_script_encoding", "utf-8"), self.config.get("utc_timestamp", False))
        self._execution_log("- Created file '%s'" % (new_file), log_level_limit=1)

    def _migrate(self):
        destination_version = self._get_destination_version()
        current_version = self.sgdb.get_current_schema_version()

        # do it!
        self._execute_migrations(current_version, destination_version)

    def _get_destination_version(self):
        label_version = self.config.get("label_version", None)
        schema_version = self.config.get("schema_version", None)

        destination_version = None
        destination_version_by_label = None
        destination_version_by_schema = None

        if label_version is not None:
            destination_version_by_label = self.sgdb.get_version_number_from_label(label_version)
            """
            if specified label exists at database and schema version was not specified,
            is equivalent to run simple-db-migrate with schema_version equals to the version with specified label
            """
            if destination_version_by_label is not None and schema_version is None:
                schema_version = destination_version_by_label
                self.config.update("schema_version", destination_version_by_label)

        if schema_version is not None and self.sgdb.get_version_id_from_version_number(schema_version):
            destination_version_by_schema = schema_version

        if label_version is None:
            if schema_version is None:
                destination_version = self.db_migrate.latest_version_available()
            elif destination_version_by_schema is None:
                destination_version = schema_version
            else:
                destination_version = destination_version_by_schema
        else:
            if schema_version is None:
                destination_version = self.db_migrate.latest_version_available()
            elif (destination_version_by_label is None) or (destination_version_by_schema == destination_version_by_label):
                destination_version = schema_version

        if (destination_version_by_schema is not None) and (destination_version_by_label is not None) and (destination_version_by_schema != destination_version_by_label):
            raise Exception("label (%s) and schema_version (%s) don't correspond to the same version at database" % (label_version, schema_version))

        if (schema_version is not None and label_version is not None) and ((destination_version_by_schema is not None and destination_version_by_label is None) or (destination_version_by_schema is None and destination_version_by_label is not None)):
            raise Exception("label (%s) or schema_version (%s), only one of them exists in the database" % (label_version, schema_version))

        if destination_version is not '0' and not (self.db_migrate.check_if_version_exists(destination_version) or self.sgdb.get_version_id_from_version_number(destination_version)):
            raise Exception("version not found (%s)" % destination_version)

        return destination_version

    def _get_migration_files_to_be_executed(self, current_version, destination_version, is_migration_up):
        if current_version == destination_version and not self.config.get("force_execute_old_migrations_versions", False):
            return []

        schema_migrations = self.sgdb.get_all_schema_migrations()

        # migration up
        if is_migration_up:
            available_migrations = self.db_migrate.get_all_migrations()
            remaining_migrations = Lists.subtract(available_migrations, schema_migrations)
            remaining_migrations_to_execute = [migration for migration in remaining_migrations if migration.version <= destination_version]
            return remaining_migrations_to_execute

        # migration down...
        destination_version_id = self.sgdb.get_version_id_from_version_number(destination_version)
        try:
            migration_versions = self.db_migrate.get_all_migration_versions()
        except:
            migration_versions = []
        down_migrations_to_execute = [migration for migration in schema_migrations if migration.id > destination_version_id]
        force_files = self.config.get("force_use_files_on_down", False)
        for migration in down_migrations_to_execute:
            if not migration.sql_down or force_files:
                if migration.version not in migration_versions:
                    raise Exception("impossible to migrate down: one of the versions was not found (%s)" % migration.version)
                migration_tmp = self.db_migrate.get_migration_from_version_number(migration.version)
                migration.sql_up = migration_tmp.sql_up
                migration.sql_down = migration_tmp.sql_down
                migration.file_name = migration_tmp.file_name

        down_migrations_to_execute.reverse()
        return down_migrations_to_execute

    def _execute_migrations(self, current_version, destination_version):
        """
        passed a version:
            this version don't exists in the database and is younger than the last version -> do migrations up until this version
            this version don't exists in the database and is older than the last version -> do nothing, is a unpredictable behavior
            this version exists in the database and is older than the last version -> do migrations down until this version

        didn't pass a version -> do migrations up until the last available version
        """

        is_migration_up = True
        # check if a version was passed to the program
        if self.config.get("schema_version"):
            # if was passed and this version is present in the database, check if is older than the current version
            destination_version_id = self.sgdb.get_version_id_from_version_number(destination_version)
            if destination_version_id:
                current_version_id = self.sgdb.get_version_id_from_version_number(current_version)
                # if this version is previous to the current version in database, then will be done a migration down to this version
                if current_version_id > destination_version_id:
                    is_migration_up = False
            # if was passed and this version is not present in the database and is older than the current version, raise an exception
            # cause is trying to go down to something that never was done
            elif current_version > destination_version:
                raise Exception("Trying to migrate to a lower version wich is not found on database (%s)" % destination_version)

        # getting only the migration sql files to be executed
        migrations_to_be_executed = self._get_migration_files_to_be_executed(current_version, destination_version, is_migration_up)

        self._execution_log("- Current version is: %s" % current_version, "GREEN", log_level_limit=1)

        if migrations_to_be_executed is None or len(migrations_to_be_executed) == 0:
            self._execution_log("- Destination version is: %s" % current_version, "GREEN", log_level_limit=1)
            self._execution_log("\nNothing to do.\n", "PINK", log_level_limit=1)
            return

        self._execution_log("- Destination version is: %s" % (is_migration_up and migrations_to_be_executed[-1].version or destination_version), "GREEN", log_level_limit=1)

        up_down_label = is_migration_up and "up" or "down"
        if self.config.get("show_sql_only", False):
            self._execution_log("\nWARNING: database migrations are not being executed ('--showsqlonly' activated)", "YELLOW", log_level_limit=1)
        else:
            self._execution_log("\nStarting migration %s!" % up_down_label, log_level_limit=1)

        self._execution_log("*** versions: %s\n" % ([ migration.version for migration in migrations_to_be_executed]), "CYAN", log_level_limit=1)

        sql_statements_executed = []
        for migration in migrations_to_be_executed:
            sql = is_migration_up and migration.sql_up or migration.sql_down

            if not self.config.get("show_sql_only", False):
                self._execution_log("===== executing %s (%s) =====" % (migration.file_name, up_down_label), log_level_limit=1)

                label = None
                if is_migration_up:
                    label = self.config.get("label_version", None)

                try:
                    self.sgdb.change(sql, migration.version, migration.file_name, migration.sql_up, migration.sql_down, is_migration_up, self._execution_log, label)
                except Exception, e:
                    self._execution_log("===== ERROR executing %s (%s) =====" % (migration.abspath, up_down_label), log_level_limit=1)
                    raise e

                # paused mode
                if self.config.get("paused_mode", False):
                    raw_input("* press <enter> to continue... ")

            # recording the last statement executed
            sql_statements_executed.append(sql)

        if self.config.get("show_sql", False) or self.config.get("show_sql_only", False):
            self._execution_log("__________ SQL statements executed __________", "YELLOW", log_level_limit=1)
            for sql in sql_statements_executed:
                self._execution_log(sql, "YELLOW", log_level_limit=1)
            self._execution_log("_____________________________________________", "YELLOW", log_level_limit=1)

    def _execution_log(self, msg, color="CYAN", log_level_limit=2):
        if self.config.get("log_level", 1) >= log_level_limit:
            CLI.msg(msg, color)
        self.log.debug(msg)

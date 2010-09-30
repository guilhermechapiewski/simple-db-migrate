from cli import CLI
from core import Migration, SimpleDBMigrate
from helpers import Lists
from mysql import MySQL

class Main(object):

    def __init__(self, config=None, mysql=None, db_migrate=None):
        self.cli = CLI()
        self.config = config or {}

        self.mysql = mysql
        if self.mysql is None and not self.config.get("new_migration"):
            self.mysql = MySQL(config)

        self.db_migrate = db_migrate or SimpleDBMigrate(config)

    def execute(self):
        self.cli.msg("\nStarting DB migration...", "PINK")
        if self.config.get("new_migration"):
            self.create_migration()
        else:
            self.migrate()
        self.cli.msg("\nDone.\n", "PINK")

    def create_migration(self):
        # TODO: create file in the migrations directory, not in current
        new_file = Migration.create(self.config.get("new_migration"))
        self.cli.msg("- Created file '%s'" % (new_file))

    def migrate(self):
        destination_version = self.get_destination_version()
        current_version = self.mysql.get_current_schema_version()

        # do it!
        self.execute_migrations(current_version, destination_version)

    def get_destination_version(self):
        destination_version = self.config.get("schema_version")
        if destination_version is None:
            destination_version = self.db_migrate.latest_version_available()

        if destination_version is not '0' and not self.db_migrate.check_if_version_exists(destination_version):
            raise Exception("version not found (%s)" % destination_version)

        return destination_version

    def get_migration_files_to_be_executed(self, current_version, destination_version, is_migration_up):
        mysql_versions = self.mysql.get_all_schema_versions()
        migration_versions = self.db_migrate.get_all_migration_versions()

        # migration up
        if is_migration_up:
            remaining_versions_to_execute = Lists.subtract(migration_versions, mysql_versions)
            remaining_migrations_to_execute = [self.db_migrate.get_migration_from_version_number(version) for version in remaining_versions_to_execute if version <= destination_version]
            return remaining_migrations_to_execute

        # migration down...
        destination_version_id = self.mysql.get_version_id_from_version_number(destination_version)
        migrations = self.mysql.get_all_schema_migrations()
        down_migrations_to_execute = [migration for migration in migrations if migration.id > destination_version_id]
        for migration in down_migrations_to_execute:
            if not migration.sql_down:
                if migration.version not in migration_versions:
                    raise Exception("impossible to migrate down: one of the versions was not found (%s)" % migration.version)
                migration_tmp = self.db_migrate.get_migration_from_version_number(migration.version)
                migration.sql_up = migration_tmp.sql_up
                migration.sql_down = migration_tmp.sql_down
                migration.file_name = migration_tmp.file_name

        down_migrations_to_execute.reverse()
        return down_migrations_to_execute

    def execute_migrations(self, current_version, destination_version):
        """
        passed a version:
            this version don't exists in the database and is younger than the last version -> do migrations up until the this version
            this version don't exists in the database and is older than the last version -> do nothing, is a unpredictable behavior
            this version exists in the database and is older than the last version -> do migrations down until this version

        didn't pass a version -> do migrations up until the last available version
        """

        is_migration_up = True
        # check if a version was passed to the program
        if self.config.get("schema_version"):
            # if was passed and this version is present in the database, check if is older than the current version
            destination_version_id = self.mysql.get_version_id_from_version_number(destination_version)
            if destination_version_id:
                current_version_id = self.mysql.get_version_id_from_version_number(current_version)
                # if this version is previous to the current version in database, then will be done a migration down to this version
                if current_version_id > destination_version_id:
                    is_migration_up = False
            # if was passed and this version is not present in the database and is older than the current version, raise an exception
            # cause is trying to go down to something that never was done
            elif current_version > destination_version:
                raise Exception("Trying to migrate to a lower version wich is not found on database (%s)" % destination_version)

        # getting only the migration sql files to be executed
        migrations_to_be_executed = self.get_migration_files_to_be_executed(current_version, destination_version, is_migration_up)

        self.cli.msg("- Current version is: %s" % current_version, "GREEN")

        if migrations_to_be_executed is None or len(migrations_to_be_executed) == 0:
            self.cli.msg("- Destination version is: %s" % current_version, "GREEN")
            self.cli.msg("\nNothing to do.\n", "PINK")
            return

        self.cli.msg("- Destination version is: %s" % (is_migration_up and migrations_to_be_executed[-1].version or destination_version), "GREEN")

        up_down_label = is_migration_up and "up" or "down"
        if self.config.get("show_sql_only"):
            self.cli.msg("\nWARNING: database migrations are not being executed ('--showsqlonly' activated)", "YELLOW")
        else:
            self.cli.msg("\nStarting migration %s!" % up_down_label)

        if self.config.get("log_level") >= 1:
            self.cli.msg("*** versions: %s\n" % ([ migration.version for migration in migrations_to_be_executed]), "CYAN")

        sql_statements_executed = []
        for migration in migrations_to_be_executed:
            sql = is_migration_up and migration.sql_up or migration.sql_down

            if not self.config.get("show_sql_only"):
                if self.config.get("log_level") >= 1:
                    self.cli.msg("===== executing %s (%s) =====" % (migration.file_name, up_down_label))

                log = None
                if self.config.get("log_level") >= 2:
                    log = self.cli.msg

                self.mysql.change(sql, migration.version, migration.file_name, migration.sql_up, migration.sql_down, is_migration_up, execution_log=log)

                # paused mode
                if self.config.get("paused_mode"):
                    raw_input("* press <enter> to continue... ")

            # recording the last statement executed
            sql_statements_executed.append(sql)

        if self.config.get("show_sql") or self.config.get("show_sql_only"):
            self.cli.msg("__________ SQL statements executed __________", "YELLOW")
            for sql in sql_statements_executed:
                self.cli.msg(sql, "YELLOW")
            self.cli.msg("_____________________________________________", "YELLOW")


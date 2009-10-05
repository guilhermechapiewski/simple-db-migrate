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

        self.cli.msg("- Current version is: %s" % current_version, "GREEN")
        self.cli.msg("- Destination version is: %s" % destination_version, "GREEN")

        # if current and destination versions are the same,
        # will consider a migration up to execute remaining files
        is_migration_up = True
        if int(current_version) > int(destination_version):
            is_migration_up = False

        # do it!
        self.execute_migrations(current_version, destination_version, is_migration_up)

    def get_destination_version(self):
        destination_version = self.config.get("schema_version")
        if destination_version is None:
            destination_version = self.db_migrate.latest_version_available()

        if not self.db_migrate.check_if_version_exists(destination_version):
            raise Exception("version not found (%s)" % destination_version)

        return destination_version

    def get_migration_files_to_be_executed(self, current_version, destination_version):
        mysql_versions = self.mysql.get_all_schema_versions()
        migration_versions = self.db_migrate.get_all_migration_versions()

        # migration up: the easy part
        if current_version <= destination_version:
            remaining_versions_to_execute = Lists.subtract(migration_versions, mysql_versions)
            remaining_versions_to_execute = [version for version in remaining_versions_to_execute if version <= destination_version]
            return remaining_versions_to_execute

        # migration down...
        down_versions = [version for version in mysql_versions if version <= current_version and version > destination_version]
        for version in down_versions:
            if version not in migration_versions:
                raise Exception("impossible to migrate down: one of the versions was not found (%s)" % version)
        down_versions.reverse()
        return down_versions

    def execute_migrations(self, current_version, destination_version, is_migration_up):
        # getting only the migration sql files to be executed
        versions_to_be_executed = self.get_migration_files_to_be_executed(current_version, destination_version)

        if versions_to_be_executed is None or len(versions_to_be_executed) == 0:
            self.cli.msg("\nNothing to do.\n", "PINK")
            return

        up_down_label = is_migration_up and "up" or "down"
        if self.config.get("show_sql_only"):
            self.cli.msg("\nWARNING: database migrations are not being executed ('--showsqlonly' activated)", "YELLOW")
        else:
            self.cli.msg("\nStarting migration %s!" % up_down_label)
        
        if self.config.get("log_level") >= 1:
            self.cli.msg("*** versions: %s\n" % versions_to_be_executed, "CYAN")

        sql_statements_executed = []
        for migration_version in versions_to_be_executed:
            migration = self.db_migrate.get_migration_from_version_number(migration_version)
            sql = is_migration_up and migration.sql_up or migration.sql_down

            if not self.config.get("show_sql_only"):
                if self.config.get("log_level") >= 1:
                    self.cli.msg("===== executing %s (%s) =====" % (migration.file_name, up_down_label))
                
                log = None
                if self.config.get("log_level") >= 2:
                    log = self.cli.msg
                
                self.mysql.change(sql, migration_version, is_migration_up, execution_log=log)

            #recording the last statement executed
            sql_statements_executed.append(sql)

        if self.config.get("show_sql") or self.config.get("show_sql_only"):
            self.cli.msg("__________ SQL statements executed __________", "YELLOW")
            for sql in sql_statements_executed:
                self.cli.msg(sql, "YELLOW")
            self.cli.msg("_____________________________________________", "YELLOW")


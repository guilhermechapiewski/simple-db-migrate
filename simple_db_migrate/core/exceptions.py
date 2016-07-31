from simple_db_migrate.helpers import Utils

class MigrationException(Exception):
    def __init__(self, msg=None, sql=None):
        self.msg = msg
        if not msg:
            self.msg = 'error executing migration'
        self.sql = sql

    def __str__(self):
        if self.sql:
            self.details = '[ERROR DETAILS] SQL command was:\n%s' % Utils.encode(self.sql, "utf-8")
            return '%s\n\n%s' % (Utils.encode(self.msg, "utf-8"), self.details)

        return self.msg

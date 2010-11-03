class MigrationException(Exception):
    def __init__(self, msg=None, sql=None):
        self.msg = msg
        if not msg:
            self.msg = 'error executing migration'
        self.sql = sql
        
    def __str__(self):
        if self.sql:
            self.details = '[ERROR DETAILS] SQL command was:\n%s' % self.sql
            return '%s\n\n%s' % (self.msg, self.details)
    
        return self.msg

from optparse import OptionParser

class CLI(object):

    def start(self):
        parser = OptionParser()

        parser.add_option("-v", "--version", 
                dest="schema_version", 
                default=None, 
                help="Schema version to migrate to. If not provided will migrate to the last version available.")
                
        parser.add_option("-c", "--config", 
                dest="db_config_file", 
                default="simple-db-migrate.conf", 
                help="Use specific config file. If not provided, will use simple-db-migrate.conf that is located in the current directory.")
                
        parser.add_option("-d", "--dir", 
                dest="migrations_dir", 
                default=".", 
                help="Find migration files in a specific directory. If not provided will search for files in the current directory.")

        return parser.parse_args()

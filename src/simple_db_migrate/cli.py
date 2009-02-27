from optparse import OptionParser

class CLI(object):

    def __init__(self):
        self.__config_parser()

    def __config_parser(self):
        self.__parser = OptionParser()

        self.__parser.add_option("-v", "--version", 
                dest="schema_version", 
                default=None, 
                help="Schema version to migrate to. If not provided will migrate to the last version available.")
                
        self.__parser.add_option("-c", "--config", 
                dest="db_config_file", 
                default="simple-db-migrate.conf", 
                help="Use specific config file. If not provided, will use simple-db-migrate.conf that is located in the current directory.")
                
        self.__parser.add_option("-d", "--dir", 
                dest="migrations_dir", 
                default=".", 
                help="Find migration files in a specific directory. If not provided will search for files in the current directory.")
                
        self.__parser.add_option("--showsql", 
                action="store_true", 
                dest="show_sql", 
                default=False, 
                help="Show all SQL statements executed.")

    def get_parser(self):
        return self.__parser

    def parse(self):
        return self.__parser.parse_args()

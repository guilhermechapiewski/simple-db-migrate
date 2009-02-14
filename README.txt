##################################################

./db-migrate --config=example/simple-db-migrate.conf --dir=example

##################################################

Usage: db-migrate [options]

Options:
  -h, --help            show this help message and exit
  --version=SCHEMA_VERSION
                        Schema version to migrate to. If not provided will
                        migrate to the last version available.
  --config=DB_CONFIG_FILE
                        Use specific config file. If not provided, will use
                        simple-db-migrate.conf that is located in the current
                        directory.
  --dir=MIGRATIONS_DIR  Find migration files in a specific directory. If not
                        provided will search for files in the current
                        directory.



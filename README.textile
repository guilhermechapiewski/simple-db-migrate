h1. simple-db-migrate "quick" documentation

h2. Quick start

simple-db-migrate is damn simple. The best way to understand how it works is by installing and using it.

You can install it by typing:

<pre>
    $ pip install simple-db-migrate
</pre>

After installing, for usage tips type:

<pre>
    $ db-migrate --help
</pre>

h2. Upgrading simple-db-migrate version

The 1.5.0 version removed some legacy code which updates the version table from the old format to the actual.
To whom use simple-db-migrate for MySQL or Oracle with version less than 1.4.1.1, and the Bruno Caimar version for MS-SQL Server should use the version 1.4.4 before use the new version, to have the version table updated.

h2. Understanding how it works

The first thing you'll need is a migration file. There are some example migration files in the "example" directory. The migration files have the following format:

<pre>
    SQL_UP = """
    CREATE TABLE aleatory (
      id int(11) NOT NULL auto_increment,
      name varchar(255) default NULL,
      PRIMARY KEY  (id)
    );
    """
    SQL_DOWN = """
    DROP TABLE aleatory;
    """

... where SQL_UP and SQL_DOWN are two strings that contains respectively the SQL statements to upgrade and downgrade the database schema.
</pre>

You can use db-migrate to create new migrations by typing:

<pre>
    $ db-migrate --create create_table_users
</pre>

The file names need to respect the format "YYYYMMDDHHMMSS_migration_description.migration". simple-db-migrate uses the YYYYMMDDHHMMSS information to track the database schema version and to decide the order of execution of the scripts. simple-db-migrate will go through all .migration files in your directory and execute all of them in their creation (date) order.

Second, you have to configure access to your database so simple-db-migrate can execute DDL. Just create a file named "simple-db-migrate.conf", with the following content (there is also an example in the "example" directory):

<pre>
    DATABASE_HOST = "localhost"
    DATABASE_USER = "root"
    DATABASE_PASSWORD = ""
    DATABASE_NAME = "migration_example"
    DATABASE_MIGRATIONS_DIR = "."

*** The MIGRATIONS_DIR directive may have relative (from the location of this file) or absolute paths separated by ':' pointing to the migrations directories.
</pre>

You don't need to create the database. simple-db-migrate will create it for you.

After these two things you are ready to go. Just navigate to the directory where you created your configuration file and type:

<pre>
    $ db-migrate
</pre>

If you don't want to navigate to the directory, you can specify it path instead. In this case you will also need to specify the path to the config file. Note that this also makes it possible to use any name you like for the config file:

<pre>
    $ db-migrate --config=path/to/file.conf
</pre>

h2. Migrating to a specific version

If you want, you can migrate your database schema to a specific version by supplying the --migration (or -m) parameter. The version id is the YYYYMMDDHHMMSS identifier used at the migration file:

<pre>
    $ db-migrate --migration=20090227000129
</pre>

If you don't specify any version, simple-db-migrate will migrate the schema to the latest version available in the migrations directories specified in the config file.

h2. Configuring multiple database environments

If you want to use the same configuration file for multiple environments you can prefix the names with the name of your environment and specify it when executing, like the example below.

<pre>
    DATABASE_HOST = "localhost"                              # default database host
    STAGING_DATABASE_HOST = "staging.host.com"               # staging database host
    PRODUCTION_DATABASE_HOST = "production.host.com"         # production database host
    DATABASE_USER = "root"
    DATABASE_PASSWORD = ""
    DATABASE_NAME = "migration_example"
    DATABASE_MIGRATIONS_DIR = "."

    $ db-migrate --config=path/to/file.conf                  # will use default configurations
    $ db-migrate --config=path/to/file.conf --env=staging    # will use staging configurations, and default to keys not prefixed
    $ db-migrate --config=path/to/file.conf --env=production # will use production configurations, and default to keys not prefixed
</pre>

h2. Available configurations

You can set default values for internal configurations in your configuration file and overwrite (some of them) using the command line parameters. Below is a list of all configuration options.

(head). | Configuration | description | default value | possible values |
| DATABASE_HOST | hostname where database is located | - | - |
| DATABASE_PORT | port where database is located | - | - |
| DATABASE_USER | username used to connect to database and execute the commands | - | - |
| DATABASE_PASSWORD | password used to connect to database and execute the commands | - | - |
| DATABASE_NAME | database name used where the commands will be executed | - | - |
| DATABASE_ENGINE | the database type where migrations will be executed | mysql | oracle,mysql,mssql  |
| DATABASE_VERSION_TABLE | the table name used to save database versions | __db_version__ | any name supported by the database |
| UTC_TIMESTAMP | create migration files using UTC time to format the name | False | True,False |
| DATABASE_MIGRATIONS_DIR | directories to look for migration files separated by _:_ | - | - |
| DATABASE_ENCODING | encoding used on database | utf-8 | any valid encoding |
| DATABASE_SCRIPT_ENCODING | encoding used on migration files | utf-8 | any valid encoding |
| drop_db_first | if True drop the database before executing migrations | False | True,False |

| force_execute_old_migrations_versions | if True and current and destination database versions are equal, execute any old migrations not executed yet | False | True,False |
| force_use_files_on_down | if True use SQL_DOWN from migration files instead of that present on version table | False | True,False |
| label_version | label to be applied to all executed migrations when doing a upgrade on database | - | - |
| log_dir | directory where a file will be created with a full log of the process, with the current time as name | - | - |
| new_migration | name for the migration to be created | - | any alpha numeric word, without spaces |
| paused_mode | execute migrations, pausing after finishing each one | False | True,False |
| schema_version | the desired version of the database, will do a upgrade or a downgrade to be sure that this will be the current version of database | - | - |
| show_sql | if True show executed SQL commands | False | True,False |
| show_sql_only | if True only show the SQL, but do not execute them | False | True,False |

h2. Supported databases engines

You can use this project to run migrations on MySQL, Oracle and MS-SQL server databases.
The default database engine is MySQL. To use the other databases set the DATABASE_ENGINE constant in the configuration file.

h2. Procedure, Function, Trigger (Oracle and MySQL), and Packages (Oracle) support

You can use db-migrate to manage procedures, functions, triggers and packages using "/" as final delimiter.
Examples:

```
-- Oracle
CREATE OR REPLACE FUNCTION simple
RETURN VARCHAR2 IS
BEGIN
    RETURN 'Simple Function';
END simple;
/

-- MySQL
CREATE PROCEDURE country_hos
(IN con CHAR(20))
BEGIN
  SELECT Name, HeadOfState FROM Country
  WHERE Continent = con;
END
/
```

h2. Roadmap, bug reporting and feature requests

For detailed info about future versions, bug reporting and feature requests, go to "issues":https://github.com/guilhermechapiewski/simple-db-migrate/issues page.

h2. Other questions

Mail me at "guilherme.chapiewski at gmail.com" for further questions.

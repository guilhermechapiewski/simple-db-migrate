from setuptools import setup, find_packages
import re

import simple_db_migrate

setup(
    name = "simple-db-migrate",
    version = simple_db_migrate.SIMPLE_DB_MIGRATE_VERSION,
    packages = find_packages(),
    author = "Guilherme Chapiewski",
    author_email = "guilherme.chapiewski@gmail.com",
    description = "simple-db-migrate is a database versioning and migration tool inspired on Rails Migrations.",
    license = "Apache License 2.0",
    keywords = "database migration tool mysql",
    url = "http://guilhermechapiewski.github.com/simple-db-migrate/",
    long_description = "simple-db-migrate is a database versioning and migration tool inspired on Rails Migrations. This tool helps you easily refactor, manage and track your database schema. The main difference is that Rails migrations are intended to be used only on Ruby projects while simple-db-migrate makes it possible to have migrations in any language (Java, Python, Ruby, PHP, whatever). This is possible because simple-db-migrate uses database's DDL (Data Definition Language) to do the database operations, while Rails Migrations are written in a Ruby internal DSL.",
    
    # generate script automatically
    entry_points = {
        'console_scripts': [
            'db-migrate = simple_db_migrate:run',
        ],
    },

)

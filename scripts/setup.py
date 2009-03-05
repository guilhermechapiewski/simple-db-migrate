from setuptools import setup, find_packages

setup(
    name = "SimpleDBMigrate",
    version = "1.0",
    packages = find_packages("src"),
    package_dir = {"":"src"},
    scripts = ["src/db-migrate"],
    
    #install_requires = ["mysql==1.2.2"], #TODO: install mysql as dependency

    author = "Guilherme Chapiewski",
    author_email = "guilherme.chapiewski@gmail.com",
    description = "simple-db-migrate is a database versioning and migration tool inspired on Rails Migrations.",
    license = "Apache License 2.0",
    keywords = "database migration tool mysql",
    url = "http://simple-db-migrate.lighthouseapp.com",
    long_description = "simple-db-migrate is a database versioning and migration tool inspired on Rails Migrations. This tool helps you easily refactor, manage and track your database schema. The main difference is that Rails migrations are intended to be used only on Ruby projects while simple-db-migrate makes it possible to have migrations in any language (Java, Python, Ruby, PHP, whatever). This is possible because simple-db-migrate uses database's DDL (Data Definition Language) to do the database operations, while Rails Migrations are written in a Ruby internal DSL.",
    download_url = "http://github.com/guilhermechapiewski/simple-db-migrate",
)

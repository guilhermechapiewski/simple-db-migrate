# Makefile for simple-db-migrate

help:
	@echo
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean      to clean garbage left by builds and installation"
	@echo "  compile    to compile .py files (just to check for syntax errors)"
	@echo "  unit       to execute all db-migrate unit tests"
	@echo "  func       to execute all db-migrate functional tests"
	@echo "  install    to install simple-db-migrate"
	@echo "  build      to build without installing simple-db-migrate"
	@echo "  dist       to create egg for distribution"
	@echo "  publish    to publish the package to PyPI"
	@echo

clean:
	@echo "Cleaning..."
	@rm -rf build dist src/simple_db_migrate.egg-info simple_db_migrate.egg-info *.pyc **/*.pyc *~ *.migration *.foo
	@#removing test temp files
	@rm -rf `date +%Y`*

compile: clean
	@echo "Compiling source code..."
	@rm -rf src/*.pyc
	@rm -rf tests/*.pyc
	@python -tt -m compileall src
	@python -tt -m compileall tests

metrics:
	@pylint db_migrate.ui db_migrate.domain
	@pyflakes db_migrate/ui db_migrate/domain

unit: compile
	@make clean
	@echo "Starting tests..."
	@nosetests -s --verbose --with-coverage --cover-erase --cover-package=db_migrate tests/unit/*
	@make clean

func: compile
	@make clean
	@echo "Starting tests..."
	@nosetests -s --verbose --with-coverage --cover-erase --cover-package=db_migrate tests/functional/*
	@make clean

install:
	@/usr/bin/env python ./setup.py install

build:
	@/usr/bin/env python ./setup.py build

dist: clean
	@python setup.py sdist

publish: dist
	@python setup.py sdist upload

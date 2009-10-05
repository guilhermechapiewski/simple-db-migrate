# Makefile for simple-db-migrate

help:
	@echo
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean      to clean garbage left by builds and installation"
	@echo "  compile    to compile .py files (just to check for syntax errors)"
	@echo "  test       to execute all simple-db-migrate tests"
	@echo "  install    to install simple-db-migrate"
	@echo "  build      to build without installing simple-db-migrate"
	@echo "  publish    to publish the package to PyPI"
	@echo

clean:
	@echo "Cleaning..."
	@git clean -df > /dev/null
	@rm -rf build dist src/simple_db_migrate.egg-info simple_db_migrate.egg-info *.pyc **/*.pyc *~ *.migration *.foo

compile: clean
	@echo "Compiling source code..."
	@rm -rf src/*.pyc
	@rm -rf tests/*.pyc
	@python -tt -m compileall src
	@python -tt -m compileall tests

test: compile
	@make clean
	@echo "Starting tests..."
	@nosetests -s --verbose --with-coverage --cover-erase --cover-package=cli,config,core,helpers,main,mysql tests/*
	@make clean

install:
	@/usr/bin/env python ./setup.py install

build:
	@/usr/bin/env python ./setup.py build

publish:
	@python setup.py bdist_egg upload

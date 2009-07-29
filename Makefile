# Makefile for simple-db-migrate

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean      to clean garbage left by builds and installation"
	@echo "  test       to execute all simple-db-migrate tests"
	@echo "  install    to install simple-db-migrate"
	@echo "  build      to build without installing simple-db-migrate"
	@echo "  publish    to publish the package to PyPI"
	@echo " "

clean:
	@echo "Cleaning..."
	@rm -rf build dist src/simple_db_migrate.egg-info simple_db_migrate.egg-info *.pyc **/*.pyc *~ *.migration *.foo

test:
	@make clean
	@echo "Starting tests..."
	@nosetests -s --verbose --with-coverage --cover-erase --cover-package=cli,config,core,helpers,main,mysql tests/* > /dev/null
	@#nosetests -s --verbose --with-coverage --cover-erase --cover-inclusive tests/* > /dev/null
	@make clean

install:
	@/usr/bin/env python ./setup.py install

build:
	@/usr/bin/env python ./setup.py build

publish:
	@python setup.py bdist_egg upload

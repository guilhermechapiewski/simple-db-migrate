# Makefile for simple-db-migrate

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean      to clean garbage left by builds and installation"
	@echo "  test       to execute all simple-db-migrate tests"
	@echo "  coverage   to execute simple-db-migrate coverage report"
	@echo "  install    to install simple-db-migrate"
	@echo "  build      to build without installing simple-db-migrate"
	@echo "  publish    to publish the package to PyPI"
	@echo " "

clean:
	@echo "Cleaning garbage..."
	@rm -rf build dist src/simple_db_migrate.egg-info simple_db_migrate.egg-info *.pyc *~ *.migration *.foo
	@echo "Done."

test:
	@make clean
	@./scripts/run_tests.sh
	@make clean

coverage:
	@./scripts/run_coverage.sh

install:
	@/usr/bin/env python ./setup.py install

build:
	@/usr/bin/env python ./setup.py build

publish:
	@python setup.py bdist_egg upload

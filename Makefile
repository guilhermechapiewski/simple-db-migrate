# Makefile for simple-db-migrate

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean      to clean garbage"
	@echo "  test       to execute all simple-db-migrate tests"
	@echo "  coverage   to execute simple-db-migrate coverage report"
	@echo "  install    to install simple-db-migrate"
	@echo " "

clean:
	rm -rf build dist src/SimpleDBMigrate.egg-info *.pyc *~

test:
	./scripts/run_tests.sh

coverage:
	./scripts/run_coverage.sh

install:
	/usr/bin/env python ./setup.py install

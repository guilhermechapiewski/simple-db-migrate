# Makefile for simple-db-migrate

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean      to clean garbage"
	@echo "  test       to execute all simple-db-migrate tests"
	@echo "  coverage   to execute simple-db-migrate coverage report"
	@echo "  install    to install simple-db-migrate"
	@echo " "

clean:
	@echo "Cleaning garbage..."
	@rm -rf build dist src/simple_db_migrate.egg-info *.pyc *~
	@echo "Done."

test:
	@./scripts/run_tests.sh

coverage:
	@./scripts/run_coverage.sh

install:
	@/usr/bin/env python ./setup.py install

publish:
	@python setup.py bdist_egg upload

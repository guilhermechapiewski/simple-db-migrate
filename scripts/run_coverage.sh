#!/bin/sh

coverage -e
coverage -x tests/test.py > /dev/null
echo "\n"
coverage -r src/simple_db_migrate/*.py
echo "\n"

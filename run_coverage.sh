#!/bin/sh

coverage -e
coverage -x tests/test.py > /dev/null
echo "\n"
coverage -r *.py
echo "\n"

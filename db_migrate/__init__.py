#!/usr/bin/env python
# encoding: utf-8

from db_migrate.ui.console import Console

__version__ = "2.0.0"
version = __version__
Version = __version__

def run():
    console = Console()
    console.run()

if __name__ == '__main__':
    run()
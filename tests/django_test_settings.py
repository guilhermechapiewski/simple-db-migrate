INSTALLED_APPS = [
    'simple_db_migrate.db_migrate'
]
SECRET_KEY = 'secret_key'

import django

if hasattr(django, 'setup'):
    django.setup()

#!/usr/bin/env python
# encoding: utf-8

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations, verify

from db_migrate.config import Config
import db_migrate.config as conf

def test_can_create_config_with_a_dictionary_and_it_has_keys_as_properties():
    config = Config(a='b', c='d', e='f')
    
    assert config
    
    assert config.a == 'b'
    assert config.c == 'd'
    assert config.e == 'f'

@with_fakes
@with_patched_object(conf, 'load_module', Fake(callable=True))
def test_config_can_load_from_file_db_migrate_properties():
    clear_expectations()

    fake_conf = Fake('conf')
    fake_conf.has_attr(
        DATABASE_HOST="myHost",
        DATABASE_USER = "myUser",
        DATABASE_PASSWORD = "myPass",
        DATABASE_NAME = "myDb",
        DATABASE_PORT = 1111,
        DATABASE_MIGRATIONS_DIR = 'myDir'
    )

    conf.load_module.with_args('some_path').returns(fake_conf)

    config = Config.load_file('some_path')

    assert config.host == 'myHost'
    assert config.db == 'myDb'
    assert config.user == 'myUser'
    assert config.password == 'myPass'
    assert config.port == 1111
    assert config.migrations_dir == 'myDir'

def test_config_can_get_static_instance():
    config = Config.static(some="config", goes="here")
    
    assert config.some == 'config'
    assert config.goes == 'here'

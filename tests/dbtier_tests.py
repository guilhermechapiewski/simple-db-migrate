# coding: utf-8

from fudge import Fake, with_fakes, with_patched_object, clear_expectations
from fudge.inspector import arg

from src.dbtier import DbTier
from src.core.exceptions import MigrationException

def get_tier():
    config = Fake('config')
    driver = Fake('driver')
    tier = DbTier(config, driver)

    return tier, config, driver

@with_fakes
def test_new_db_tier_keeps_track_of_db_driver():
    clear_expectations()

    tier, config, driver = get_tier()

    assert tier.db_driver == driver

@with_fakes
@with_patched_object(DbTier, 'create_primary_key_in_versions_table', Fake(callable=True))
def test_verify_db_consistency_calls_the_right_methods():
    clear_expectations()

    tier, config, driver = get_tier()

    tier.verify_db_consistency()

@with_fakes
def test_create_primary_key_in_versions_table_finishes_if_everything_works():
    clear_expectations()

    tier, config, driver = get_tier()

    driver.expects('execute').with_args('alter table version add id int(11) primary key auto_increment not null;')

    tier.create_primary_key_in_versions_table()

@with_fakes
def test_create_primary_key_in_versions_table_does_nothing_if_fails():
    clear_expectations()

    tier, config, driver = get_tier()

    driver.expects('execute').with_args('alter table version add id int(11) primary key auto_increment not null;').raises(MigrationException())

    tier.create_primary_key_in_versions_table()

@with_fakes
def test_drop_database_if_everything_works():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('db_name').returns('myDb')

    driver.expects('execute').with_args('set foreign_key_checks=0; drop database if exists myDb;')

    tier.drop_db()

@with_fakes
def test_drop_database_raises_migration_error_when_exception():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('db_name').returns('myDb')

    driver.expects('execute').with_args('set foreign_key_checks=0; drop database if exists myDb;').raises(MigrationException())

    try:
        tier.drop_db()
    except MigrationException, m:
        assert str(m) == "can't drop database 'myDb'; database doesn't exist"
        return

    assert False, "should not have gotten this far"

@with_fakes
@with_patched_object(DbTier, 'drop_db', Fake(callable=True))
@with_patched_object(DbTier, 'create_db', Fake(callable=True))
def test_initialize_db_calls_drop_if_config_says_to():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('drop_db_first').returns(True)

    tier.initialize_db()

@with_fakes
@with_patched_object(DbTier, 'create_db', Fake(callable=True))
def test_initialize_db_does_not_call_drop_if_config_says_not_to():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('drop_db_first').returns(False)

    tier.initialize_db()

@with_fakes
def test_create_db():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('db_name').returns('myDb')

    driver.expects('execute').with_args("create database if not exists myDb;")

    tier.create_db()

@with_fakes
def test_create_version_table():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('db_version_table').returns('versions')

    driver.expects('execute').with_args('create table if not exists versions ( version varchar(20) NOT NULL default \"0\" );')

    tier.create_version_table()

@with_fakes
def test_that_no_migration_zero_is_inserted_if_some_migration_is_there():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('db_version_table').returns('versions')

    driver.expects('query_scalar').with_args('select count(*) from versions;').returns(1)

    tier.verify_if_migration_zero_is_present()

@with_fakes
def test_that_migration_zero_is_inserted_when_needed():
    clear_expectations()

    tier, config, driver = get_tier()

    config.expects('get').with_args('db_version_table').returns('versions')

    driver.expects('query_scalar').with_args('select count(*) from versions;').returns(0)
    driver.expects('execute').with_args('insert into versions (version) values (\"0\");')

    tier.verify_if_migration_zero_is_present()

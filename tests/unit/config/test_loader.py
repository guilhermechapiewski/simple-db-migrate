#!/usr/bin/env python
# encoding: utf-8

from StringIO import StringIO

from fudge import Fake, with_fakes, with_patched_object, \
                  clear_expectations

import db_migrate.config.loader as loader

@with_fakes
@with_patched_object(loader, 'exists', Fake(callable=True))
def test_loader_raises_when_file_not_found():
    clear_expectations()
    loader.exists.with_args('/tmp/fake_module.py').returns(False)
    
    try:
        loader.load_module('/tmp/fake_module.py')
    except ValueError, err:
        assert str(err) == "File not found at /tmp/fake_module.py. Can't load module."
        return

    assert False, 'Should not have gotten this far.'

@with_fakes
@with_patched_object(loader, 'exists', Fake(callable=True))
@with_patched_object(loader, 'do_open', Fake(callable=True))
@with_patched_object(loader, 'imp', Fake('imp'))
@with_patched_object(loader, 'md5', Fake('md5'))
def test_loader_returns_module():
    clear_expectations()

    loader.exists.with_args('/tmp/my_module.py').returns(True)

    md5_instance = Fake('md5_instance')
    loader.md5.expects('new').with_args('/tmp/my_module.py').returns(md5_instance)
    md5_instance.expects('hexdigest').returns('md5hash')

    file_like = StringIO('some code')

    loader.do_open.with_args('/tmp/my_module.py', 'rb').returns(file_like)

    loader.imp.expects('load_source') \
              .with_args('md5hash', '/tmp/my_module.py', file_like) \
              .returns('my compiled module')

    module = loader.load_module('/tmp/my_module.py')

    assert module == 'my compiled module'
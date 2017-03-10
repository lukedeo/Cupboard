from __future__ import unicode_literals

import pytest
import os
from cupboard import Cupboard
from redis import ConnectionError


def test_redis_unavail():
    with pytest.raises(ConnectionError):
        d = Cupboard(host='localhost', port=8000, backend='redis')


def test_redis_avail():
    d = Cupboard(host='localhost', backend='redis')


def test_leveldb_no_create_if_missing():
    with pytest.raises(Exception):
        d = Cupboard(name='/tmp/bad', backend='leveldb')


def test_leveldb_with_create_if_missing():
    d = Cupboard(name='/tmp/good', backend='leveldb', create_if_missing=True)


def test_leveldb_lock_violation():
    with pytest.raises(IOError):
        d = Cupboard(name='/tmp/lvlexists', backend='leveldb',
                     create_if_missing=True)
        o = Cupboard(name='/tmp/lvlexists', backend='leveldb')


def test_leveldb_no_lock_violation():
    d = Cupboard(name='/tmp/lvlexists', backend='leveldb', create_if_missing=True)
    d._db.close()
    o = Cupboard(name='/tmp/lvlexists', backend='leveldb')


def test_lmdb_create():
    o = Cupboard(path='/tmp/lmexists', backend='lmdb')


def test_env_create():

    os.environ['CUPBOARD_BACKEND'] = 'redis'
    d = Cupboard()

    os.environ['CUPBOARD_BACKEND'] = 'leveldb'
    d = Cupboard(name='/tmp/envlvlexists', create_if_missing=True)

    os.environ['CUPBOARD_BACKEND'] = 'lmdb'
    d = Cupboard(path='/tmp/envlmexists')

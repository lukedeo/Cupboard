#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: _backend.py
description: backend implementations 
author: Luke de Oliveira (lukedeo@vaitech.io)
"""

from __future__ import unicode_literals

from builtins import zip
from builtins import map
from future.utils import native


# unit stuff for lmdb map size
MB = 1048576
GB = 1024 * MB
TB = 1024 * GB

AVAILABLE_BACKENDS = ['redis', 'lmdb', 'leveldb']
POSSIBLE_BACKENDS = AVAILABLE_BACKENDS[:]


class BackendUnavailable(ImportError):
    pass


class ResourceUnavailable(Exception):
    pass


BACKEND_OPS = ['write', 'create', 'batchwriter', 'reader', 'keys', 'values',
               'items', 'iteritems', 'delete', 'rmkeys', 'close']


def _backend_unavailable(backend):
    def _unavail(*args, **kwargs):
        raise BackendUnavailable('backend: {} is not available'.format(backend))


def _to_string(o):
    return _obj_to_pkl_string(o, _hp)


# creation routines
def _lmdb_create(*args, **kwargs):
    if 'map_size' not in list(kwargs.keys()):
        kwargs['map_size'] = 10 * GB
    return lmdb.open(*args, **kwargs)


def _leveldb_create(*args, **kwargs):
    return plyvel.DB(*args, **kwargs)


def _redis_create(*args, **kwargs):
    ctx = redis.StrictRedis(*args, **kwargs)
    if not ctx.ping():
        raise ResourceUnavailable('redis unavailable-is redis-server running?')
    return ctx


# destruction routines
def _lmdb_rmkeys(db):
    for key in _lmdb_keys(db, lambda x: x):
        with db.begin(write=True, buffers=True) as txn:
            txn.delete(key)


def _leveldb_rmkeys(db):
    for key in [key for key, _ in db.iterator()]:
        db.delete(key)


def _redis_rmkeys(db):
    db.flushdb()


# closing routines
def _lmdb_close(db):
    db.close()


def _leveldb_close(db):
    db.close()


def _redis_close(db):
    # what does it mean to "close" redis, anyways?
    pass


# write routines
def _lmdb_write(db, *args, **kwargs):
    with db.begin(write=True) as txn:
        txn.put(*args, **kwargs)


def _leveldb_write(db, *args, **kwargs):
    if kwargs:
        raise NotImplementedError('passing keyword arguments to '
                                  'LevelDB not supported')
    db.put(*list(map(native, args)))


def _redis_write(db, *args, **kwargs):
    db.set(*args, **kwargs)


# delete routines
def _lmdb_delete(db, *args, **kwargs):
    with db.begin(write=True, buffers=True) as txn:
        txn.delete(*args, **kwargs)


def _leveldb_delete(db, *args, **kwargs):
    if kwargs:
        raise NotImplementedError('passing keyword arguments to '
                                  'LevelDB not supported')
    db.delete(*list(map(native, args)))


def _redis_delete(db, *args, **kwargs):
    db.delete(*args, **kwargs)


# read routines
def _lmdb_reader(db, *args, **kwargs):
    with db.begin(write=False) as txn:
        v = txn.get(*args, **kwargs)
    return v


def _leveldb_reader(db, *args, **kwargs):
    if kwargs:
        raise NotImplementedError('passing keyword arguments to '
                                  'LevelDB not supported')
    return db.get(*list(map(native, args)))


def _redis_reader(db, *args, **kwargs):
    return db.get(*args, **kwargs)


# keys
def _lmdb_keys(db, projexpr):
    ks = []
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for key, _ in cursor:
                ks.append(projexpr(key))
    return ks


def _leveldb_keys(db, projexpr):
    return [projexpr(key) for key, _ in db.iterator()]


def _redis_keys(db, projexpr):
    return list(map(projexpr, db.keys()))


# values
def _lmdb_values(db, projexpr):
    vs = []
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for _, val in cursor:
                vs.append(projexpr(val))
    return vs


def _leveldb_values(db, projexpr):
    return [projexpr(value) for _, value in db.iterator()]


def _redis_values(db, projexpr):
    return list(map(projexpr, db.mget(list(db.keys()))))


# items
def _lmdb_items(db, projexpr):
    vs = []
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for key, val in cursor:
                vs.append((projexpr(key), projexpr(val)))
    return vs


def _leveldb_items(db, projexpr):
    return [(projexpr(key), projexpr(value)) for key, value in db.iterator()]


def _redis_items(db, projexpr):
    ks = list(db.keys())
    return list(zip(list(map(projexpr, ks)), list(map(projexpr, db.mget(ks)))))


# iteritems
def _lmdb_iteritems(db, projexpr):
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for key, val in cursor:
                yield (projexpr(key), projexpr(val))


def _leveldb_iteritems(db, projexpr):
    return ((projexpr(key), projexpr(value)) for key, value in db.iterator())


def _redis_iteritems(db, projexpr):
    for key in db.scan_iter(match=None, count=None):
        yield (projexpr(key), projexpr(db.get(key)))


# write routines

def _lmdb_batchwriter(db, projexpr, writer, iterable):
    with db.begin(write=False) as writer:
        for k, v in iterable:
            projexpr(k, v)
    writer = db


def _leveldb_batchwriter(db, projexpr, writer, iterable):
    with db.write_batch(transaction=True) as writer:
        for k, v in iterable:
            projexpr(k, v)
    writer = db


def _redis_batchwriter(db, projexpr, writer, iterable):
    writer = db.pipeline()
    for k, v in iterable:
        projexpr(k, v)
    writer.execute()
    writer = db


UNAVAILABLE_BACKENDS = []
try:
    import redis
except ImportError as e:
    UNAVAILABLE_BACKENDS.append('redis')

try:
    import plyvel
except ImportError as e:
    UNAVAILABLE_BACKENDS.append('leveldb')

try:
    import lmdb
except ImportError as e:
    UNAVAILABLE_BACKENDS.append('lmdb')

AVAILABLE_BACKENDS = [
    _ for _ in AVAILABLE_BACKENDS
    if _ not in UNAVAILABLE_BACKENDS
]

for be in UNAVAILABLE_BACKENDS:
    for op in BACKEND_OPS:
        exec('_{}_{} = _backend_unavailable("{}")'.format(op, be, be))

__all__ = ['AVAILABLE_BACKENDS', 'POSSIBLE_BACKENDS',
           'BACKEND_OPS', 'UNAVAILABLE_BACKENDS']

__all__ += [
    '_{}_{}'.format(b, o)
    for o in BACKEND_OPS
    for b in AVAILABLE_BACKENDS
]

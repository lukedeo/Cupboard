from cPickle import dumps as _obj_to_pkl_string
from cPickle import loads as _obj_from_pkl_string
from cPickle import HIGHEST_PROTOCOL as _hp

# unit stuff for lmdb map size
MB = 1048576
GB = 1024 * MB
TB = 1024 * GB

AVAILABLE_BACKENDS = ['redis', 'lmdb', 'leveldb']


class BackendUnavailable(ImportError):
    pass


# to dump things to pickle for insertion

_OPS = ['write', 'create', 'batchwriter', 'reader', 'keys', 'values', 'items']


def _backend_unavailable(backend):
    def _unavail(*args, **kwargs):
        raise BackendUnavailable('backend: {} is not available'.format(backend))


def _to_string(o):
    return _obj_to_pkl_string(o, _hp)


# creation routines
def _redis_create(*args, **kwargs):
    return redis.StrictRedis(*args, **kwargs)


def _leveldb_create(*args, **kwargs):
    return plyvel.DB(*args, **kwargs)


def _lmdb_create(*args, **kwargs):
    if 'map_size' not in kwargs.keys():
        kwargs['map_size'] = 10 * GB
    return lmdb.open(*args, **kwargs)


# write routines
def _redis_write(db, *args):
    db.set(*args)


def _leveldb_write(db, *args):
    db.put(*args)


def _lmdb_write(db, *args):
    with db.begin(write=True) as txn:
        txn.put(*args)


# read routines
def _redis_reader(db, *args):
    db.get(*args)


def _leveldb_reader(db, *args):
    db.get(*args)


def _lmdb_reader(db, *args):
    with db.begin(write=False) as txn:
        v = txn.get(*args)
    return v


# keys
def _lmdb_keys(db, call):
    ks = []
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for key, _ in cursor:
                ks.append(call(key))
    return ks


def _leveldb_keys(db, call):
    return [call(key) for key, _ in db.RangeIter()]


def _redis_keys(db, call):
    return map(call, db.keys())


# values
def _lmdb_values(db, call):
    vs = []
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for _, val in cursor:
                vs.append(call(val))
    return vs


def _leveldb_values(db, call):
    return [call(value) for _, value in db.RangeIter()]


def _redis_values(db, call):
    return map(call, db.mget(db.keys()))


# items
def _lmdb_items(db, call):
    vs = []
    with db.begin(write=False) as txn:
        with txn.cursor() as cursor:
            for key, val in cursor:
                vs.append((call(key), call(val)))
    return vs


def _leveldb_items(db, call):
    return [(call(key), call(value)) for key, value in db.RangeIter()]


def _redis_items(db, call):
    ks = db.keys()
    return zip(map(call, db.mget(ks)), map(call, ks))


# write routines
def _redis_batchwriter(db, call, writer, iterable):
    writer = db.pipeline()
    for k, v in iterable:
        call(k, v)
    writer.execute()
    writer = db


def _leveldb_batchwriter(db, call, writer, iterable):
    with db.write_batch(transaction=True) as writer:
        for k, v in iterable:
            call(k, v)
    writer = db


def _lmdb_batchwriter(db, call, writer, iterable):
    with db.begin(write=False) as writer:
        for k, v in iterable:
            call(k, v)
    writer = db

UNAVAILABLE_BACKENDS = []
try:
    import redis
except ImportError, e:
    UNAVAILABLE_BACKENDS.append('redis')

try:
    import plyvel
except ImportError, e:
    UNAVAILABLE_BACKENDS.append('leveldb')

try:
    import lmdb
except ImportError, e:
    UNAVAILABLE_BACKENDS.append('lmdb')

AVAILABLE_BACKENDS = [
    _ for _ in AVAILABLE_BACKENDS if _ not in UNAVAILABLE_BACKENDS]

for be in UNAVAILABLE_BACKENDS:
    for op in _OPS:
        exec('_{}_{} = _backend_unavailable("{}")'.format(op, be, be))

__all__ = ['UNAVAILABLE_BACKENDS', '_to_string', '_obj_from_pkl_string'] + \
    ['_{}_{}'.format(be, op) for op in _OPS for be in AVAILABLE_BACKENDS]

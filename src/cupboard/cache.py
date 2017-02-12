import os

from ._backend import *

import numpy as np

# from common import _NUMPY_IDENTIFIER, _PICKLE_IDENTIFIER

BACKEND = os.environ.get('CACHE_BACKEND', 'lmdb')

_NUMPY_IDENTIFIER = bytes('NP||')
_PICKLE_IDENTIFIER = bytes('PKL||')

assert BACKEND in {'redis', 'leveldb', 'lmdb'}, "Backend must be one of {redis, leveldb, lmdb}"

# unit stuff for lmdb map size
MB = 1048576
GB = 1024 * MB
TB = 1024 * GB

# to dump things to pickle for insertion


class Cupboard(object):
    """
    Example:

    >>> d = Cupboard(*args)
    >>> d['foo'] = 'bar'
    >>> d['bar'] = np.array([2, 3])
    >>> d['bar'] + 3
    ... array([5, 6])
    """

    def __init__(self, *args, **kwargs):
        # make a DB instance

        if 'backend' in kwargs:
            self._backend = kwargs.pop('backend')
        else:
            self._backend = BACKEND

        # create the actual callables dependent on the backend
        for func in ['write', 'create', 'batchwriter', 'reader', 'keys', 'values', 'items', 'delete']:
            exec('self._db_{} = _{}_{}'.format(func, self._backend, func))

        self._db = self._db_create(*args, **kwargs)

        # get an obj reference for batch writes (later)
        self._write_obj = self._db

        self._buffer = None
        self._key_ptr = None

    @property
    def _stager(self):
        pass

    @_stager.setter
    def _stager(self, o):
        # make double sure we have a numpy obj
        if hasattr(o, 'tostring') and isinstance(o, np.ndarray):
            self._buffer = _NUMPY_IDENTIFIER + o.tostring()

        elif not isinstance(o, basestring):
            self._buffer = bytes(_PICKLE_IDENTIFIER + _to_string(o))

        else:
            self._buffer = bytes(o)

        self._db_write(self._write_obj, self._key_ptr, self._buffer)

    @_stager.getter
    def _stager(self):
        return self._reconstruct_obj(self._buffer)

    @staticmethod
    def _reconstruct_obj(buf):
        if _NUMPY_IDENTIFIER in bytes(buf):
            return np.fromstring(buf.replace(_NUMPY_IDENTIFIER, ''))
        if _PICKLE_IDENTIFIER in bytes(buf):
            return _obj_from_pkl_string(buf.replace(_PICKLE_IDENTIFIER, ''))
        if buf == 'none':
            return None
        return buf

    def __getitem__(self, key):
        # if hasattr(key, '__iter__') and not isinstance(key, basestring):
        #     return [self.__getitem__(k) for k in key]
        self._key_ptr = bytes(key)
        # self._buffer = self._db.get(self._key_ptr)
        self._buffer = self._db_reader(self._db, self._key_ptr)

        if self._stager is None:
            if key not in self._db_keys(self._db, self._reconstruct_obj):
                raise KeyError('key: {} not found in storage'.format(key))

        return self._stager

    def get(self, key, replacement=None):
        # if hasattr(key, '__iter__') and not isinstance(key, basestring):
        #     return [self.__getitem__(k) for k in key]

        self._key_ptr = bytes(key)
        # self._buffer = self._db.get(self._key_ptr)
        self._buffer = self._db_reader(self._db, self._key_ptr)

        if self._stager is None:
            if key not in self._db_keys(self._db, self._reconstruct_obj):
                return replacement

        return self._stager

    def delete(self, key):
        self._key_ptr = bytes(key)
        self._db_delete(self._db, self._key_ptr)

    def __setitem__(self, key, o):
        self._key_ptr = bytes(key)
        self._stager = o

    def __delitem__(self, key):
        self.delete(key)

    def items(self):
        return self._db_items(self._db, self._reconstruct_obj)

    def keys(self):
        return self._db_keys(self._db, self._reconstruct_obj)

    def values(self):
        return self._db_values(self._db, self._reconstruct_obj)

    def batch_set(self, iterable):
        self._db_batchwriter(self._db, self.__setitem__,
                             self._write_obj, iterable)

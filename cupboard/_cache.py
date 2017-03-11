#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: _cache.py
description: actual implementation of wrapper & logic
author: Luke de Oliveira (lukedeo@vaitech.io)
"""

from __future__ import unicode_literals
from __future__ import print_function
from builtins import object
from contextlib import contextmanager
import os

# get everything def'd in __all__ inside _backend.py
from ._backend import *
from ._marshal import MarshalHandler, AVAILABLE_PROTOCOLS

BACKEND = lambda: os.environ.get('CUPBOARD_BACKEND')

if BACKEND() is None:
    import logging
    logger = logging.getLogger(__name__)

    BACKEND = lambda: os.environ.get('CUPBOARD_BACKEND', AVAILABLE_BACKENDS[0])

    logger.warning('Falling back to backend: {}'.format(BACKEND))

assert BACKEND() in POSSIBLE_BACKENDS, \
    "Backend must be one of {}".format(set(POSSIBLE_BACKENDS))

# unit stuff for lmdb map size
MB = 1048576
GB = 1024 * MB
TB = 1024 * GB


class Cupboard(object):
    """ 
    Creates a new cupboard instance.

    Can be backed by one of {lmdb, redis, leveldb}.

    The backend can be specified one of two ways. First, `backend` keyword 
    argument can be passed to the Cupboard constructor. Second, the 
    CUPBOARD_BACKEND env var can be set. Note that the keyword argument takes 
    priority in the event that both are present.


    *NOTE* values are returned AS COPIES, therefore in-place ops on values will
    NOT result in a change, as they are IMMUTABLE


    Redis Args:
    -----------
        *args and **kwargs are forwarded to redis.StrictRedis(...)

        Requires a running redis server or similar

        For a more detailed description consult:
        https://redis-py.readthedocs.io/en/latest/#redis.StrictRedis

        host: The host of the DB, defaults to 'localhost'

        port: Port to access the DB, defaults to 6379

        db: DB number to access, defaults to 0


    LMDB Args:
    ----------
        *args and **kwargs are forwarded to lmdb.Environment(...)

        For a more detailed description, consult:
        https://lmdb.readthedocs.io/en/release/#lmdb.Environment

        The two arguments most users will change are...

        path: Location of directory (if subdir=True) or file prefix to 
            store the database.

        map_size: Maximum size database may grow to; used to size the memory 
            mapping. If database grows larger than map_size, an exception will 
            be raised and the user must close and reopen Environment. On 64-bit 
            there is no penalty for making this huge (say 1TB). 
            Must be <2GB on 32-bit.

    LevelDB Args:
    -------------
        *args and **kwargs are forwarded to plyvel.DB(...)

        For a more detailed description, consult:
        https://plyvel.readthedocs.io/en/latest/api.html#DB

        name (str): name of the database (directory name)

        create_if_missing (bool): whether a new database should be created 
            if needed

    Returns:
    --------
        A cupboard.Cupboard instance

    Example:
    --------

        >>> d = Cupboard('meta.db', create_if_missing=True, backend='leveldb')
        >>> d['author'] = 'John Smith'
        >>> d['info'] = {'age': 42, 'favorite_function': np.mean}
        >>> d['info']['favorite_function']([1, 2, 3])
        2

    Raises:
    -------
        Asserts that the backend requested is valid and available given the 
        installed Python packages. Raises ResourceUnavailable if using redis and
        the connection is unreachable with a PING.

    """

    def __init__(self, *args, **kwargs):
        # make a DB instance

        if 'backend' in kwargs:
            _backend = kwargs.pop('backend')
            assert _backend in AVAILABLE_BACKENDS, \
                "Backend must be one of {}".format(set(AVAILABLE_BACKENDS))
            self._backend = _backend
        else:
            self._backend = BACKEND()

        # create the actual callables dependent on the backend
        for func in BACKEND_OPS:
            exec('self._db_{} = _{}_{}'.format(func, self._backend, func))

        self._db = self._db_create(*args, **kwargs)

        # get an obj reference for batch writes (later)
        self._write_obj = self._db

        self._buffer = None
        self._key_ptr = None
        self.__additional_args = {}

        self._M = MarshalHandler()

    @contextmanager
    def marshal_as(self, protocol):
        if protocol not in AVAILABLE_PROTOCOLS:
            raise ValueError('{} not a valid protocol'.format(protocol))
        orig_protocol = self._M.protocol
        self._M.protocol = protocol
        yield protocol
        self._M.protocol = orig_protocol

    @contextmanager
    def pass_arguments(self, **kwargs):
        self.__additional_args.update(kwargs)
        yield
        self.__additional_args = {}

    @property
    def _stager(self):
        return self._reconstruct_obj(self._buffer)

    @_stager.setter
    def _stager(self, o):
        self._buffer = self._M.marshal(o)
        self._db_write(self._write_obj, self._key_ptr,
                       self._buffer, **self.__additional_args)

    # @staticmethod
    def _reconstruct_obj(self, buf):
        return self._M.unmarshal(buf)

    def __getitem__(self, key):
        self._key_ptr = self._M.marshal(key, override='auto', ensure_immutable=True)
        self._buffer = self._db_reader(self._db, self._key_ptr, **self.__additional_args)

        if self._stager is None:
            if key not in self._db_keys(self._db, self._reconstruct_obj):
                raise KeyError('key: {} not found in storage'.format(key))

        return self._stager

    def get(self, key, replacement=None):
        self._key_ptr = self._M.marshal(key, override='auto', ensure_immutable=True)
        self._buffer = self._db_reader(self._db, self._key_ptr, **self.__additional_args)

        if self._stager is None:
            if key not in self._db_keys(self._db, self._reconstruct_obj):
                return replacement

        return self._stager

    def delete(self, key):
        self._key_ptr = self._M.marshal(key, override='auto', ensure_immutable=True)
        self._db_delete(self._db, self._key_ptr, **self.__additional_args)

    def __setitem__(self, key, o):
        self._key_ptr = self._M.marshal(key, override='auto', ensure_immutable=True)
        self._stager = o

    def __delitem__(self, key):
        self.delete(key)

    def items(self):
        return self._db_items(self._db, self._reconstruct_obj)

    def iteritems(self):
        return self._db_iteritems(self._db, self._reconstruct_obj)

    def keys(self):
        return self._db_keys(self._db, self._reconstruct_obj)

    def values(self):
        return self._db_values(self._db, self._reconstruct_obj)

    def close(self):
        return self._db_close(self._db)

    def rmkeys(self):
        return self._db_rmkeys(self._db)

    def batch_set(self, iterable):
        self._db_batchwriter(self._db, self.__setitem__,
                             self._write_obj, iterable)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: store.py
description: actual implementation of wrapper & logic
author: Luke de Oliveira (lukedeo@vaitech.io)
"""

from __future__ import unicode_literals
from __future__ import print_function
from builtins import object
from contextlib import contextmanager
from functools import wraps
import inspect
import logging
import os
import time

logger = logging.getLogger(__name__)

# get everything def'd in __all__ inside _backend.py
from .backend import *
from .marshal import MarshalHandler, AVAILABLE_PROTOCOLS


def default_backend():
    """
    Returns the backend defined by the `CUPBOARD_BACKEND` environment variable.
    Returns one of the available backends if not set.
    """
    backend = os.environ.get('CUPBOARD_BACKEND')
    if backend is None:
        backend = available_backends()[0]
        logger.warning('Falling back to backend: {}'.format(backend))
    return backend

assert default_backend() in available_backends(), \
    "Backend must be one of {}".format(set(available_backends()))

# unit stuff for lmdb map size
MB = 1048576
GB = 1024 * MB
TB = 1024 * GB


class ExpiringValue(object):
    """container class for an object that expires"""

    def __init__(self, value, timeout):
        self._value = value
        self._timeout = timeout
        self._created = time.time()

    @property
    def expired(self):
        return (time.time() - self._created) > self._timeout

    @property
    def value(self):
        return self._value


class Cupboard(object):
    """ 
    Creates a new Cupboard instance.

    Can be backed by one of `{lmdb, redis, leveldb}`.

    The backend can be specified one of two ways. First, the `backend` keyword 
    argument can be passed to the Cupboard constructor. Second, the 
    `CUPBOARD_BACKEND` env var can be set. Note that the keyword argument takes 
    priority in the event that both are present.


    *NOTE* values are returned **AS COPIES**, therefore in-place ops on values will
    *not* result in a change, as they are **immutable**.

    Provides standard dictionary style semantics regarding the `...[...]` 
    operator. I.e., 

        #!python
        d[key] = value
        some_var = d[key]
        del d[key]

    are all valid operations on a `cupboard.Cupboard` instance.
    """

    def __init__(self, *args, **kwargs):
        """



        Redis Args:
        ----------

        `*args` and `**kwargs` are forwarded to `redis.StrictRedis(...)`

        Requires a running redis server or similar.

        For a more detailed description consult the [redis-py docs](https://redis-py.readthedocs.io/en/latest/#redis.StrictRedis).

        * `host (str)`: The host of the DB, defaults to `'localhost'`

        * `port (int)`: Port to access the DB, defaults to `6379`

        * `db (int)`: DB number to access, defaults to `0`


        LMDB Args:
        ----------
        `*args` and `**kwargs` are forwarded to `lmdb.Environment(...)`

        For a more detailed description, consult the [LMDB documentation](https://lmdb.readthedocs.io/en/release/#lmdb.Environment).

        The two arguments most users will change are...

        * `path (str)`: Location of directory (if `subdir=True`) or file prefix to 
            store the database.

        * `map_size (int)`: Maximum size database may grow to; used to size the memory 
            mapping. If database grows larger than map_size, an exception will 
            be raised and the user must close and reopen Environment. On 64-bit 
            there is no penalty for making this huge (say 1TB). 
            Must be <2GB on 32-bit.

        LevelDB Args:
        -------------

        `*args` and `**kwargs` are forwarded to `plyvel.DB(...)`

        For a more detailed description, consult the [Plyvel documentation](https://plyvel.readthedocs.io/en/latest/api.html#DB)

        * `name (str)`: name of the database (directory name)

        * `create_if_missing (bool)`: whether a new database should be created if needed

        Returns:
        --------

        A `cupboard.Cupboard` instance


        Example:
        --------

            #!python
            d = Cupboard(
                name='meta.db', 
                create_if_missing=True, 
                backend='leveldb'
            )
            d['author'] = 'John Smith'
            d['info'] = {
                'age': 42, 
                'favorite_function': np.mean
            }

            f = d['info']['favorite_function']
            assert f([1, 2, 3]) == 2

        Raises:
        -------

        Asserts that the backend requested is valid and available given the 
        installed Python packages. Raises `ResourceUnavailable` if using redis and
        the connection is unreachable with a `PING`.
        """

        if 'backend' in kwargs:
            _backend = kwargs.pop('backend')
            assert _backend in available_backends(), \
                "Backend must be one of {}".format(set(available_backends()))
            self._backend = _backend
        else:
            self._backend = default_backend()

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
        """ Allows marshalling using different protocols for handling the 
        transport of data to different key value stores.

        Data can be stored as a pickle string, a json string, a gzipped json 
        string, a byte string, or a gzipped byte string.


        Args:
        -----

        * `protocol (str)`: one of `auto`, `pickle`, `json`, `jsongz`, 
            `bytes`, `bytesgz`. `auto` let's cupboard choose the best 
            marshalling option

        Raises:
        -------
            `ValueError` if a valid marshalling protocol is not specified.
        """

        if protocol not in AVAILABLE_PROTOCOLS:
            raise ValueError('{} not a valid protocol'.format(protocol))
        orig_protocol = self._M.protocol
        self._M.protocol = protocol
        yield protocol
        self._M.protocol = orig_protocol

    @contextmanager
    def pass_arguments(self, **kwargs):
        """
        Pass keyword arguments to the underlying backend implementation of a 
        `put`, `set` or what have you.
        """
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
        self._key_ptr = self._M.marshal(
            key, override='auto', ensure_immutable=True)
        self._buffer = self._db_reader(
            self._db, self._key_ptr, **self.__additional_args)

        if self._stager is None:
            if key not in self._db_keys(self._db, self._reconstruct_obj):
                raise KeyError('key: {} not found in storage'.format(key))

        return self._stager

    def get(self, key, replacement=None):
        """
        Get the value associated with the key `key`. If not present, will return 
        `replacement` in it's stead (default `None`).
        """
        self._key_ptr = self._M.marshal(
            key, override='auto', ensure_immutable=True)
        self._buffer = self._db_reader(
            self._db, self._key_ptr, **self.__additional_args)

        if self._stager is None:
            if key not in self._db_keys(self._db, self._reconstruct_obj):
                return replacement

        return self._stager

    def delete(self, key):
        """
        Delete the `(key, value)` pair associated with the passed in `key`.
        """
        self._key_ptr = self._M.marshal(
            key, override='auto', ensure_immutable=True)
        self._db_delete(self._db, self._key_ptr, **self.__additional_args)

    def __setitem__(self, key, o):
        self._key_ptr = self._M.marshal(
            key, override='auto', ensure_immutable=True)
        self._stager = o

    def __delitem__(self, key):
        self.delete(key)

    def items(self):
        """
        Returns a Python 2.7 style list of `(key, value)` pairs
        """
        return self._db_items(self._db, self._reconstruct_obj)

    def iteritems(self):
        """
        Returns a Python 2.7 style generator of `(key, value)` pairs
        """
        return self._db_iteritems(self._db, self._reconstruct_obj)

    def keys(self):
        """
        Returns a Python 2.7 style list of all present keys
        """
        return self._db_keys(self._db, self._reconstruct_obj)

    def values(self):
        """
        Returns a Python 2.7 style list of all present values
        """
        return self._db_values(self._db, self._reconstruct_obj)

    def close(self):
        """
        Closes the underlying database (regardless of backend)
        """
        return self._db_close(self._db)

    def rmkeys(self):
        """
        Remove all keys and `(key, value)` pairs from the 
        `cupboard.Cupboard` instance.
        """
        return self._db_rmkeys(self._db)

    def batch_set(self, iterable):
        """
        Accepts a list of `(key, value)` pairs to update the 
        `cupboard.Cupboard` instance.
        """
        self._db_batchwriter(self._db, self.__setitem__,
                             self._write_obj, iterable)

    def up(self):
        """
        Returns a boolean indicating if the backend is reachable. Always true 
        for `lmdb` and `leveldb` backends.
        """
        return self._db_up(self._db)

    def update(self, u):
        """
        Accepts a list of `(key, value)` pairs, a dictionary, or another 
        `cupboard.Cupboard` to update the parent `cupboard.Cupboard` instance.
        More formally, will update the internal storage from any other object 
        that implements a `.items()` method which returns a list of `(a, b)` 
        pairs.
        """
        if hasattr(u, 'items'):
            u = list(u.items())
        self._db_batchwriter(self._db, self.__setitem__,
                             self._write_obj, u)

    def function_cache(self, expire=None, ignore_args=None, protocol='auto'):
        """ Allows a function to be decorated in order to use the underlying 
        `cupboard.Cupboard` object to cache return values of a function

        Args:
        -----

        * `expire (numeric)`: the number of seconds to cache results of this 
            particular function for.
        * `ignore_args (string or List[string])`: any arguments 
            (listed by name) to ignore when caching the function call
        * `protocol (str)`: one of `auto`, `pickle`, `json`, `jsongz`, 
            `bytes`, `bytesgz`. `auto` let's cupboard choose the best 
            marshalling option

        Example:
        --------

            #!python
            d = Cupboard(
                name='meta.db', 
                create_if_missing=True, 
                backend='leveldb'
            )

            @d.function_cache(expire=120, ignore_args='other_arg')
            def foo(x, y, other_arg):
                print(other_arg)
                return x + y


        Raises:
        -------

        `KeyError` if `ignore_args` is not an argument to the wrapped function

        """

        class context:
            ignore_args = []
            protocol = 'auto'
            expiration_container = lambda x: x

        protocol = protocol.lower()

        if protocol not in AVAILABLE_PROTOCOLS:
            raise ValueError('protocol `{}` not valid. Choose from one of {}'
                             .format(protocol, AVAILABLE_PROTOCOLS))

        # hack for python 2.7 nonlocal equivalent
        context.ignore_args = ignore_args
        context.protocol = protocol

        if expire is not None:
            context.expiration_container = lambda x: ExpiringValue(x, expire)

        def decorator(function):

            @wraps(function)
            def func(*args, **kwargs):

                if kwargs.pop('skip_cache', False):
                    return function(*args, **kwargs)

                # Handle cases where caching is down or otherwise not
                # available.
                if not self.up():
                    return function(*args, **kwargs)

                ignore_args = context.ignore_args
                protocol = context.protocol

                cache_key_dict = inspect.getcallargs(function, *args, **kwargs)

                if ignore_args is not None:
                    if isinstance(ignore_args, str):
                        ignore_args = [ignore_args]

                    for argname in ignore_args:
                        if argname not in cache_key_dict:
                            raise KeyError(
                                'asked to ignore argument "{badarg}" that is '
                                'not present amongst valid arguments '
                                '[{arglist}] for function {fname}'.format(
                                    badarg=argname,
                                    arglist=', '.join(cache_key_dict.keys()),
                                    fname=function.__name__
                                )
                            )
                        del cache_key_dict[argname]

                cache_key_dict['fname'] = function.__name__

                cache_key = tuple(cache_key_dict.items())

                cached_result = self.get(cache_key, replacement=None)

                if cached_result is not None:
                    if isinstance(cached_result, ExpiringValue):
                        if cached_result.expired:
                            self.delete(cache_key)
                        else:
                            return cached_result.value
                    else:
                        return cached_result

                result = function(*args, **kwargs)

                with self.marshal_as(protocol):
                    self.__setitem__(
                        cache_key,
                        context.expiration_container(result)
                    )

                return result
            return func
        return decorator


__all__ = ['default_backend', 'Cupboard']

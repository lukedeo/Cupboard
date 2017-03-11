#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
file: _marshal.py
description: Tools for marshalling objects / data around
author: Luke de Oliveira (lukedeo@vaitech.io)
"""
from __future__ import unicode_literals
from __future__ import print_function

from future import standard_library

standard_library.install_aliases()

import sys

from builtins import bytes
# else:
# _bytes = bytes

# def bytes(*args, **kwargs):
#     return _bytes(*args, **kwargs)
# from past.builtins import basestring
from builtins import str
from builtins import object

from pickle import (dumps as _dumps, loads as _obj_from_pkl_string,
                    HIGHEST_PROTOCOL as _hp, PicklingError)

from functools import partial
import gzip
from json import dumps as _obj_to_json_string, loads as _obj_from_json_string
import io

AVAILABLE_PROTOCOLS = ['auto', 'pickle', 'json', 'json', 'jsongz', 'bytes',
                       'bytesgz']


def _to_gzip(value):
    out = io.BytesIO()
    _ = gzip.GzipFile(fileobj=out, mode='wb').write(value)
    return out.getvalue()


def _from_gzip(value):
    return gzip.GzipFile(fileobj=io.BytesIO(value), mode='rb').read()


def _obj_to_pkl_string(o):
    return _dumps(o, protocol=_hp)


class MarshalHandler(object):

    PICKLE_IDENTIFIER = b'PKL||'
    JSON_IDENTIFIER = b'JSN||'
    JSONGZ_IDENTIFIER = b'JSNGZ||'
    BYTES_IDENTIFIER = b'BYT||'
    BYTESGZ_IDENTIFIER = b'BYTGZ||'

    PROTOCOL_MAP = {
        PICKLE_IDENTIFIER: 'pickle',
        JSON_IDENTIFIER: 'json',
        JSONGZ_IDENTIFIER: 'jsongz',
        BYTES_IDENTIFIER: 'bytes',
        BYTESGZ_IDENTIFIER: 'bytesgz'
    }

    def __init__(self):
        self._protocol = 'auto'

        self.FWD_PROJ_EXPR = {
            'pickle': self._marshal_pickle,
            'json': self._marshal_json,
            'jsongz': partial(self._marshal_json, as_gzip=True),
            'bytes': self._marshal_bytes,
            'bytesgz': partial(self._marshal_bytes, as_gzip=True),
        }

        self.BWD_PROJ_EXPR = {
            'pickle': self._unmarshal_pickle,
            'json': self._unmarshal_json,
            'jsongz': self._unmarshal_json,
            'bytes': self._unmarshal_bytes,
            'bytesgz': self._unmarshal_bytes
        }

    @staticmethod
    def get_protocol(buf):
        if buf.startswith(b'r'):
            buf = buf[1:]
        return MarshalHandler.PROTOCOL_MAP[buf.split(b'||')[0] + b'||']

    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, value):
        if value not in AVAILABLE_PROTOCOLS:
            raise ValueError('{} not a valid protocol'.format(value))
        self._protocol = value

    def _marshal_json(self, obj, as_gzip=False):
        try:
            buf = bytes(_obj_to_json_string(obj), 'utf-8')
            # buf = _obj_to_json_string(obj)
        except TypeError as e:
            raise e
            raise TypeError('Object of class <{}> is not JSON '
                            'serializable'.format(type(obj)))
        if as_gzip:
            return self.JSONGZ_IDENTIFIER + _to_gzip(buf)
        return self.JSON_IDENTIFIER + buf

    def _marshal_bytes(self, obj, as_gzip=False):
        prepad = b''
        try:
            if isinstance(obj, bytes):
                buf = bytes(obj)
                prepad = b'r'
            elif isinstance(obj, str):
                buf = bytes(obj, 'utf-8')
            else:
                raise TypeError()
        except TypeError as e:
            raise e
            raise TypeError('Object of class <{}> is not serializable by raw '
                            'bytes'.format(type(obj)))
        if as_gzip:
            return prepad + self.BYTESGZ_IDENTIFIER + _to_gzip(buf)
        return prepad + self.BYTES_IDENTIFIER + buf

    def _marshal_pickle(self, obj):
        try:
            buf = _obj_to_pkl_string(obj)
        except (TypeError, PicklingError, AttributeError):
            raise TypeError('Object of class {} is not '
                            'pickle-able'.format(type(obj)))
        return self.PICKLE_IDENTIFIER + buf

    def _unmarshal_json(self, buf):

        if buf.startswith(self.JSON_IDENTIFIER):
            return _obj_from_json_string(str(
                buf.replace(self.JSON_IDENTIFIER, b''), 'utf-8'
            ))

        if buf.startswith(self.JSONGZ_IDENTIFIER):
            return _obj_from_json_string(str(
                _from_gzip(buf.replace(self.JSONGZ_IDENTIFIER, b'')), 'utf-8'
            ))

        raise ValueError('Cannot unmarshal with JSON protocol when '
                         'identifier is of '
                         'type <{}>'.format(self.get_protocol(buf)))

    def _unmarshal_bytes(self, buf):
        prjexpr = lambda x: str(x, 'utf-8')
        if buf.startswith(b'r'):
            buf = buf[1:]
            prjexpr = lambda x: x
        if buf.startswith(self.BYTES_IDENTIFIER):
            return prjexpr(buf.replace(self.BYTES_IDENTIFIER, b''))
        if buf.startswith(self.BYTESGZ_IDENTIFIER):
            return prjexpr(_from_gzip(buf.replace(
                self.BYTESGZ_IDENTIFIER, b''
            )))

        raise ValueError('Cannot unmarshal with raw bytes protocol when '
                         'identifier is of '
                         'type <{}>'.format(self.get_protocol(buf)))

    def _unmarshal_pickle(self, buf):
        if buf.startswith(self.PICKLE_IDENTIFIER):
            return _obj_from_pkl_string(buf.replace(
                self.PICKLE_IDENTIFIER, b''
            ))

        raise ValueError('Cannot unmarshal with raw bytes protocol when '
                         'identifier is of '
                         'type <{}>'.format(self.get_protocol(buf)))

    def marshal(self, blob, override=None, ensure_immutable=False):
        if ensure_immutable:
            try:
                _ = hash(blob)
            except TypeError as e:
                raise TypeError(
                    'Unhashable type found: <{}>'.format(type(blob)))
        if override is not None:
            old_protocol = self._protocol
            self._protocol = override
        if self._protocol == 'auto':
            if override is not None:
                self._protocol = old_protocol
            if isinstance(blob, str) or isinstance(blob, bytes):
                return self._marshal_bytes(blob)
            try:
                return self._marshal_json(blob)
            except TypeError:
                return self._marshal_pickle(blob)
        retrieved = self.FWD_PROJ_EXPR[self._protocol](blob)
        if override is not None:
            self._protocol = old_protocol
        return retrieved

    def unmarshal(self, buf, **kwargs):
        if buf is None:
            return None
        return self.BWD_PROJ_EXPR[self.get_protocol(buf)](buf, **kwargs)

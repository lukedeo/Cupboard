#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: _marshal.py
description: Tools for marshalling objects / data around
author: Luke de Oliveira (lukedeo@vaitech.io)
"""
from __future__ import print_function

from cPickle import (dumps as _dumps, loads as _obj_from_pkl_string,
                     HIGHEST_PROTOCOL as _hp, PicklingError)
from functools import partial
import gzip
from json import (dumps as _obj_to_json_string,
                  loads as _obj_from_json_string)
import StringIO


AVAILABLE_PROTOCOLS = ['auto', 'pickle', 'json', 'json', 'jsongz', 'bytes',
                       'bytesgz']


def _to_gzip(value):
    out = StringIO.StringIO()
    _ = gzip.GzipFile(fileobj=out, mode='wb').write(value)
    return out.getvalue()


def _from_gzip(value):
    return gzip.GzipFile(fileobj=StringIO.StringIO(value), mode='rb').read()


def _obj_to_pkl_string(o):
    return _dumps(o, protocol=_hp)


class MarshalHandler(object):

    PICKLE_IDENTIFIER = bytes('PKL||')
    JSON_IDENTIFIER = bytes('JSN||')
    JSONGZ_IDENTIFIER = bytes('JSNGZ||')
    BYTES_IDENTIFIER = bytes('BYT||')
    BYTESGZ_IDENTIFIER = bytes('BYTGZ||')

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
        return MarshalHandler.PROTOCOL_MAP.get(buf.split('||')[0] + '||')

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
            buf = bytes(_obj_to_json_string(obj))
        except TypeError:
            raise TypeError('Object of class {} is not json '
                            'serializable'.format(type(obj)))
        if as_gzip:
            return self.JSONGZ_IDENTIFIER + bytes(_to_gzip(buf))
        return self.JSON_IDENTIFIER + bytes(buf)

    def _marshal_bytes(self, obj, as_gzip=False):
        if not isinstance(obj, basestring):
            raise TypeError('Object of class {} is not serializable by raw '
                            'bytes'.format(type(obj)))
        if as_gzip:
            return self.BYTESGZ_IDENTIFIER + bytes(_to_gzip(bytes(obj)))
        return self.BYTES_IDENTIFIER + bytes(obj)

    def _marshal_pickle(self, obj):
        try:
            buf = bytes(_obj_to_pkl_string(obj))
        except (TypeError, PicklingError):
            raise TypeError('Object of class {} is not '
                            'pickle-able'.format(type(obj)))
        return self.PICKLE_IDENTIFIER + bytes(buf)

    def _unmarshal_json(self, buf):

        if self.JSON_IDENTIFIER in bytes(buf):
            return _obj_from_json_string(buf.replace(self.JSON_IDENTIFIER, ''))
        if self.JSONGZ_IDENTIFIER in bytes(buf):
            return _obj_from_json_string(_from_gzip(buf.replace(self.JSONGZ_IDENTIFIER, '')))
        raise ValueError('Cannot unmarshal with JSON protocol when '
                         'identifier is of '
                         'type <{}>'.format(self.get_protocol(buf)))

    def _unmarshal_bytes(self, buf):
        if self.BYTES_IDENTIFIER in bytes(buf):
            return buf.replace(self.BYTES_IDENTIFIER, '')
        if self.BYTESGZ_IDENTIFIER in bytes(buf):
            return _from_gzip(buf.replace(self.BYTESGZ_IDENTIFIER, ''))
        raise ValueError('Cannot unmarshal with raw bytes protocol when '
                         'identifier is of '
                         'type <{}>'.format(self.get_protocol(buf)))

    def _unmarshal_pickle(self, buf):
        if self.PICKLE_IDENTIFIER in bytes(buf):
            return _obj_from_pkl_string(buf.replace(self.PICKLE_IDENTIFIER, ''))
        raise ValueError('Cannot unmarshal with raw bytes protocol when '
                         'identifier is of '
                         'type <{}>'.format(self.get_protocol(buf)))

    def marshal(self, blob, override=None, ensure_immutable=False):
        if ensure_immutable:
            try:
                _ = hash(blob)
            except TypeError, e:
                raise TypeError(
                    'Unhashable type found: <{}>'.format(type(blob)))
        if override is not None:
            old_protocol = self._protocol
            self._protocol = override
        if self._protocol == 'auto':
            if override is not None:
                self._protocol = old_protocol
            if isinstance(blob, basestring):
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

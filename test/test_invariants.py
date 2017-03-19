from __future__ import unicode_literals

import pytest
import numpy as np
from cupboard import Cupboard
from redis import ConnectionError

from test_env import INVARIANT_ENVS, INVARIANT_KEYS, INVARIANT_VALUES, filename


def test_put_get_del_close():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        for value in INVARIANT_VALUES:
            for key in INVARIANT_KEYS:
                d[key] = value

            for key in INVARIANT_KEYS:
                assert(value == d[key])

            for key in INVARIANT_KEYS:
                del d[key]
        d.rmkeys()
        d.close()


def test_type_consistency():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        for value in INVARIANT_VALUES:
            for key in INVARIANT_KEYS:
                d[key] = value

                assert isinstance(d[key], type(value))

        d.rmkeys()
        d.close()


def test_marshal_as():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        with pytest.raises(ValueError):
            with d.marshal_as('notaprotocol'):
                d['thing'] = 'person'

        for k in ['a', 'b', 'c', 'd', 'e']:
            if k in list(d.keys()):
                del d[k]
        with d.marshal_as('json'):
            d['a'] = 'python is great'

        with d.marshal_as('jsongz'):
            d['b'] = 'python is great'

        with d.marshal_as('pickle'):
            d['c'] = 'python is great'

        with d.marshal_as('bytes'):
            d['d'] = 'python is great'

        with d.marshal_as('bytesgz'):
            d['e'] = 'python is great'

        assert d['a'] == d['b'] == d['c'] == d['d'] == d['e']

        with pytest.raises(TypeError):
            with d.marshal_as('json'):
                d['thing'] = np.array([1., 3., 4.])

        with pytest.raises(TypeError):
            with d.marshal_as('bytes'):
                d['thing'] = np.array([1, 3, 4])

        with pytest.raises(TypeError):
            with d.marshal_as('bytes'):
                d['thing'] = {'a': 'b'}

        with pytest.raises(TypeError):
            with d.marshal_as('pickle'):
                d['thing'] = lambda x: x
        d.rmkeys()


def test_unhashable_type():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        with pytest.raises(TypeError):
            d[[2, 3]] = 'b'

        with pytest.raises(TypeError):
            d[np.array([2, 3])] = 'b'

        with pytest.raises(TypeError):
            d[{2: 3}] = 'b'
        d.rmkeys()


def test_get_defaults():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        d['joe'] = 'person'
        d['bob'] = None
        assert d.get('joe') == 'person'
        assert d.get('joe', 'thing') == 'person'
        assert d.get('john', 'thing') == 'thing'
        assert d.get('bob') is None
        assert d.get('bob', 'thing') is None
        del d['joe']
        del d['bob']
        d.rmkeys()


def test_get_not_there():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        with pytest.raises(KeyError):
            _ = d['key']
        d.rmkeys()


def test_batch_set():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        payload = [
            ('john', np.array([1, 2, 3])),
            (4, (4, 'thing')),
            ('a', 45.3)
        ]
        # with marshal_as('pickle'):
        d.batch_set(payload)
        d.rmkeys()


def test_update():
    for env in INVARIANT_ENVS:
        for repl in INVARIANT_ENVS:
            d = env(Cupboard)
            u = repl(Cupboard)
            d.rmkeys()
            u.rmkeys()

            payload = [
                ('john', np.array([1, 2, 3])),
                (4, (4, 'thing')),
                ('a', 45.3)
            ]

            replacement_payload = [
                ('john', np.array([3, 2, 1])),
                (4, (4, 'other_thing')),
                ('b', -0.3)
            ]

            d.batch_set(payload)
            u.batch_set(replacement_payload)

            d.update(u)

            assert len(d.keys()) == 4

            assert 'b' in d.keys()

            d.rmkeys()
            u.rmkeys()


def test_delete():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        d['joe'] = 'person'
        assert d.get('joe') == 'person'
        del d['joe']
        assert d.get('joe', 'thing') == 'thing'
        d.rmkeys()


def test_iteritems_items():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()
        d['joe'] = 'person'
        d['sparky'] = 'dog'
        d['age'] = 12

        keys, values = [], []

        for k, v in d.items():
            keys.append(k)
            values.append(v)

        assert set(keys) == set(d.keys())
        assert set(values) == set(d.values())

        keys, values = [], []

        for k, v in d.iteritems():
            keys.append(k)
            values.append(v)

        assert set(keys) == set(d.keys())
        assert set(values) == set(d.values())
        d.rmkeys()

from __future__ import unicode_literals

import tempfile

from builtins import object
from builtins import bytes
import numpy as np


filename = lambda: tempfile.NamedTemporaryFile().name

INVARIANT_ENVS = (
    lambda c: c(**{
        'backend': 'redis',
        'host': 'localhost',
        'db': 0
    }),
    lambda c: c(**{
        'name': filename(),
        'create_if_missing': True,
        'backend': 'leveldb'
    }),
    lambda c: c(**{
        'path': filename(),
        'backend': 'lmdb'
    })
)


class CustomClass(object):

    def __init__(self, value=5):
        self.value = value

    def __eq__(self, o):
        return self.value == o.value


INVARIANT_KEYS = (
    'test',
    9,
    (4, 'h'),
    4.5
)

INVARIANT_VALUES = (
    'sally',
    3.45,
    (4, 5, max, str),
    {'name': 'john', (3, 4): np.mean},
    CustomClass(8),
    bytes(b'msg')
)

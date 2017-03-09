import numpy as np
import tempfile

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

INVARIANT_KEYS = (
    'test',
    9,
    (4, 'h')
)

INVARIANT_VALUES = (
    'sally',
    3.45,
    (4, 5, max, str),
    {'name': 'john', (3, 4): np.mean}
)

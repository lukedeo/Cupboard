"""
# `Cupboard`
---


`Cupboard` is an easy-to-use, near-drop-in replacement for a standard Python
dictionary. `Cupboard` provides a pythonic key-value store that can be backed
by Redis, LMDB, or LevelDB, allowing developers to write code with dictionaries
on their laptop, deploy to a phone where one might need a LevelDB type
functionality, then deploy to a Redis (Elasticache) backed environment on AWS.

_[Join us on GitHub](https://github.com/lukedeo/Cupboard) if you feel like
contributing!_

[![Build Status](https://travis-ci.org/lukedeo/Cupboard.svg)](https://travis-ci.org/lukedeo/Cupboard)
[![Coverage Status](https://coveralls.io/repos/lukedeo/Cupboard/badge.svg?branch=master&service=github)](https://coveralls.io/github/lukedeo/Cupboard?branch=master)

## Installation
---
Dependency requirements depend upon the desired backend. Technically, you can
install `Cupboard` without installing any dependencies. This is done to give
users some flexibility in differing deployment environment setups. You can
simply install `Cupboard` via

```
pip install Cupboard
```

Specific requirements of various backends are outlined below.

### Redis
You will need [`redis-server`](https://redis.io/topics/quickstart) or
equivalent running and accessable. In addition
[`redis-py`](https://github.com/andymccurdy/redis-py) is required to run.

### LevelDB
You will need [`leveldb`](https://github.com/google/leveldb) installed, with
headers in your `CPATH` set up such that you can install
[Plyvel](https://github.com/wbolster/plyvel) directly from `pip`.

### LMDB
You will simply need [`lmdb`](https://lmdb.readthedocs.io/en/release/) installed.


## 30 Seconds to `Cupboard`
---

This section will provide you with a TL;DR of what `Cupboard` allows you to do.

<!--begin_code-->
    #!python
    from cupboard import Cupboard

    import numpy as np

    d = Cupboard(
        name='/tmp/store.db',
        create_if_missing=True,
        backend='leveldb'
    )

    d['X'] = np.random.normal(0, 1, (500, 10))
    d['f'] = np.mean
    result = d['f'](d['X'])

    with d.marshal_as('jsongz'):
        d['data'] = {
            'developer': 'John Smith',
            'department': 'R&D'
        }

    assert d['data']['developer'] == 'John Smith'
    assert set(d.keys()) == set(['X', 'f', 'data'])
<!--end_code-->

Note a few important things here. Anything that can be pickled can be shuttled
around using `Cupboard`. Second, users can specify custom marshalling protocols.
In this case, we specified Gzipped JSON. All protocols are described in
`Cupboard.marshal_as`.


For another example, consider that you need to use redis to store an encrypted 
secret that needs to expire after 2 seconds. You can use the 
`Cupboard.pass_arguments` context to forward additional arguments to the underlying 
backend implementation. In the case of Redis, you're able to pass, in addition 
to other goodies, an expiration specification.

<!--begin_code-->
    #!python
    from cupboard import redis_cupboard
    import time
    from some_encryption_library import super_encrypt

    d = redis_cupboard()

    # expire after 3 seconds
    with d.pass_arguments(ex=3):
        d['{user_id}'] = super_encrypt('{user_secret}')

    assert '{user_id}' in d.keys()
    time.sleep(3)

    assert '{user_id}' not in d.keys()
<!--end_code-->

Consult the documentation for the underlying backed to find out what additional 
arguments might be useful with `Cupboard.pass_arguments` as a context.

In addition, `Cupboard` provides cache functionality with all backends via a 
decorator.

<!--begin_code-->
    #!python
    from cupboard import Cupboard
    import random

    d = Cupboard(**cup_args)

    @d.function_cache(expire=3600, ignore_args=['y', 'z'])
    def foo(x, y, z):
        return random.random()

    assert f(1, 2, 3) == f(1, 3, 2)
<!--end_code-->

## Contributing
---

As `Cupboard` is extremely early in it's development, **please** submit pull 
requests! In addition, **issues** are extremely useful this early on. In other 
words, get involved! Breaking things is ok! 

"""


import logging

# supress "No handlers could be found for logger XXXXX" error
logging.getLogger('cupboard').addHandler(logging.NullHandler())

from .store import default_backend, Cupboard
from .backend import (available_backends, BackendUnavailable,
                      ResourceUnavailable)

from .marshal import AVAILABLE_PROTOCOLS


def redis_cupboard(host='localhost', port=6379, db=0):
    """ Create a Redis-backed `cupboard.Cupboard` instance.

    Args:
    -----

    * `host (str)`: The host of the DB, defaults to `'localhost'`

    * `port (int)`: Port to access the DB, defaults to `6379`

    * `db (int)`: DB number to access, defaults to `0`

    Returns:
    --------

    returns a `cupboard.Cupboard` instance.

    Raises:
    -------
    `cupboard.ResourceUnavailable` if the Redis connection is 
    unreachable with a `PING`
    """

    return Cupboard(host=host, port=port, db=db,
                    backend='redis')


def lmdb_cupboard(path):
    """ Create a LMDB-backed `cupboard.Cupboard` instance.

    Args:
    -----

    * `path (str)`: Location of directory or file prefix to 
        store the database.

    Returns:
    --------

    returns a `cupboard.Cupboard` instance.

    """

    return Cupboard(path=path, backend='lmdb')


def leveldb_cupboard(name):
    """ Create a LevelDB-backed `cupboard.Cupboard` instance.

    Args:
    -----

    * `name (str)`: name of the database (directory name)

    Returns:
    --------

    A `cupboard.Cupboard` instance
    """
    return Cupboard(name=name, create_if_missing=True, backend='leveldb')

__all__ = [
    'default_backend',
    'Cupboard',
    'available_backends',
    # 'POSSIBLE_BACKENDS',
    # 'BACKEND_OPS',
    # 'UNAVAILABLE_BACKENDS',
    'BackendUnavailable',
    'ResourceUnavailable',
    # 'AVAILABLE_PROTOCOLS',
    'redis_cupboard',
    'lmdb_cupboard',
    'leveldb_cupboard'
]

__version__ = '0.2.1'

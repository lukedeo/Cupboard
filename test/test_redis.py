from __future__ import unicode_literals

import pytest
import time
from cupboard import Cupboard
from cupboard import POSSIBLE_BACKENDS
from redis import ConnectionError

from test_env import INVARIANT_ENVS, INVARIANT_KEYS, INVARIANT_VALUES, filename


def test_expiry():
    d = INVARIANT_ENVS[0](Cupboard)
    d.rmkeys()

    with d.pass_arguments(ex=1):
        d['a'] = 'b'

    assert d['a'] == 'b'

    time.sleep(2)
    with pytest.raises(KeyError):
        a = d['a']

    d.rmkeys()


def test_nx():
    d = INVARIANT_ENVS[0](Cupboard)
    d.rmkeys()

    with d.pass_arguments(nx=True):
        d['a'] = 'b'

    assert d['a'] == 'b'

    with d.pass_arguments(nx=True):
        d['a'] = 'other'

    assert d['a'] == 'b'

    d.rmkeys()

from __future__ import unicode_literals

import pytest
import time
from cupboard import Cupboard

from test_env import INVARIANT_ENVS, INVARIANT_KEYS, INVARIANT_VALUES, filename


def test_pass_kwargs():
    d = INVARIANT_ENVS[1](Cupboard)
    d.rmkeys()

    with d.pass_arguments(argument=1):
        with pytest.raises(NotImplementedError):
            d['a'] = 'b'

    d['a'] = 'thing'

    with d.pass_arguments(argument=1):
        with pytest.raises(NotImplementedError):
            del d['a']

    with d.pass_arguments(argument=1):
        with pytest.raises(NotImplementedError):
            _ = d['a']

    assert d['a'] == 'thing'

    d.rmkeys()

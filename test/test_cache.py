from __future__ import unicode_literals

import pytest
import numpy as np
from cupboard import Cupboard
from time import sleep as SLEEP
import random
from test_env import INVARIANT_ENVS, INVARIANT_KEYS, INVARIANT_VALUES, filename


def build_random_fn(dec):
    @dec
    def f(x, y):
        return random.random()
    return f


def test_permanent_cache():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()

        f = build_random_fn(d.function_cache())

        unique_vals = [f(2, 2) for _ in range(8)]

        num_unique_vals = len(np.unique(unique_vals))

        assert num_unique_vals == 1

        d.rmkeys()
        d.close()


def test_permanent_cache_with_skip():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()

        f = build_random_fn(d.function_cache())

        unique_vals = [f(2, 2, skip_cache=i % 2) for i in range(8)]

        num_unique_vals = len(np.unique(unique_vals))

        assert num_unique_vals == 5

        d.rmkeys()
        d.close()


def test_basic_expiring_cache():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()

        f = build_random_fn(d.function_cache(expire=0.5))

        unique_vals = [f(2, 2) for _ in range(4)]
        SLEEP(0.5)
        unique_vals.extend([f(2, 2) for _ in range(4)])

        num_unique_vals = len(np.unique(unique_vals))
        assert num_unique_vals == 2

        d.rmkeys()
        d.close()


def test_ingore_args_expiring_cache():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()

        f = build_random_fn(d.function_cache(expire=0.5, ignore_args='y'))

        unique_vals = [f(2, i) for i in range(4)]
        SLEEP(0.5)
        unique_vals.extend([f(2, i) for i in range(4)])

        num_unique_vals = len(np.unique(unique_vals))
        assert num_unique_vals == 2

        d.rmkeys()
        d.close()


def test_bad_ingore_args():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()

        with pytest.raises(KeyError):
            f = build_random_fn(d.function_cache(
                expire=0.5, ignore_args='bad_arg'
            ))
            _ = f(1, 2)

        d.rmkeys()
        d.close()


def test_bad_protocol():
    for env in INVARIANT_ENVS:
        d = env(Cupboard)
        d.rmkeys()

        with pytest.raises(ValueError):
            f = build_random_fn(d.function_cache(
                expire=0.5, protocol='notathing'
            ))

        d.rmkeys()
        d.close()

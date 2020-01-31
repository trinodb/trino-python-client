# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

This module defines exceptions for Presto operations. It follows the structure
defined in pep-0249.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from presto import exceptions
import pytest


def test_delay_exponential_without_jitter():
    max_delay = 1200.0
    get_delay = exceptions.DelayExponential(base=5, jitter=False, max_delay=max_delay)
    results = [
        10.0,
        20.0,
        40.0,
        80.0,
        160.0,
        320.0,
        640.0,
        max_delay,  # rather than 1280.0
        max_delay,  # rather than 2560.0
    ]
    for i, result in enumerate(results, start=1):
        assert get_delay(i) == result


def test_delay_exponential_with_jitter():
    max_delay = 120.0
    get_delay = exceptions.DelayExponential(base=10, jitter=False, max_delay=max_delay)
    for i in range(10):
        assert get_delay(i) <= max_delay


class SomeException(Exception):
    pass


def test_retry_with():
    max_attempts = 3
    with_retry = exceptions.retry_with(
        handle_retry=exceptions.RetryWithExponentialBackoff(),
        exceptions=[SomeException],
        conditions={},
        max_attempts=max_attempts,
    )

    class FailerUntil(object):
        def __init__(self, until=1):
            self.attempt = 0
            self._until = until

        def __call__(self):
            self.attempt += 1
            if self.attempt > self._until:
                return
            raise SomeException(self.attempt)

    with_retry(FailerUntil(2).__call__)()
    with pytest.raises(SomeException):
        with_retry(FailerUntil(3).__call__)()
